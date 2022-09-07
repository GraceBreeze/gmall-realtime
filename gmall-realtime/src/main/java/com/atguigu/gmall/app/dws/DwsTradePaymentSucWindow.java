package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TradePaymentWindowBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-26 14:37
 */
public class DwsTradePaymentSucWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取支付成功主题的数据
        String topicName = "dwd_trade_pay_detail_suc";
        String groupID = "dws_trade_payment_suc_window";
        DataStreamSource<String> payStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        // TODO 4 转化结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = payStream.map(JSON::parseObject);

        // TODO 5 去重非独立支付用户
        SingleOutputStreamOperator<TradePaymentWindowBean> payBeanStream = jsonObjStream.keyBy(jsonObj -> jsonObj.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradePaymentWindowBean>() {
                    ValueState<String> lastPayDtState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastPayDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_pay_dt", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradePaymentWindowBean> out) throws Exception {
                        String lastPayDt = lastPayDtState.value();
                        Long ts = DateFormatUtil.toTs(value.getString("pay_time"), true);
                        String payDt = DateFormatUtil.toDate(ts);
                        if (lastPayDt == null) {
                            // 首次
                            out.collect(new TradePaymentWindowBean("", "", 1L, 1L, ts));
                            lastPayDtState.update(payDt);
                        } else if (!lastPayDt.equals(payDt)) {
                            // 不是首次是独立
                            out.collect(new TradePaymentWindowBean("", "", 1L, 0L, ts));
                            lastPayDtState.update(payDt);
                        }
                    }
                });

        payBeanStream.print(">>>>>>>>");

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> beanWithWatermarkStream = payBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradePaymentWindowBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradePaymentWindowBean>() {
            @Override
            public long extractTimestamp(TradePaymentWindowBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        SingleOutputStreamOperator<TradePaymentWindowBean> reduceStream = beanWithWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean value1, TradePaymentWindowBean value2) throws Exception {
                        value1.setPaymentSucNewUserCount(value1.getPaymentSucNewUserCount() + value2.getPaymentSucNewUserCount());
                        value1.setPaymentSucUniqueUserCount(value1.getPaymentSucUniqueUserCount() + value2.getPaymentSucUniqueUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradePaymentWindowBean> values, Collector<TradePaymentWindowBean> out) throws Exception {
                        TradePaymentWindowBean tradePaymentWindowBean = values.iterator().next();
                        tradePaymentWindowBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradePaymentWindowBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradePaymentWindowBean.setTs(System.currentTimeMillis());
                        out.collect(tradePaymentWindowBean);
                    }
                });

        // TODO 7 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_trade_payment_suc_window values(?,?,?,?,?)"));

        // TODO 8 执行任务
        env.execute(groupID);
    }
}
