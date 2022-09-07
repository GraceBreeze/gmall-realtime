package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TradeOrderBean;
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
 * @create 2022-08-26 15:31
 */
public class DwsTradeOrderWindow {
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

        // TODO 3 读取kafkaDWD层对应主题数据
        String topicName = "dwd_trade_order_detail";
        String groupID = "dws_trade_order_window";
        DataStreamSource<String> orderStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        // TODO 4 转化结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = orderStream.map(JSON::parseObject);

        // TODO 5 过滤出独立下单用户和首次下单用户
        SingleOutputStreamOperator<TradeOrderBean> beanStream = jsonObjStream.keyBy(jsonObj -> jsonObj.getString("user_id"))
                .flatMap(new RichFlatMapFunction<JSONObject, TradeOrderBean>() {
                    ValueState<String> lastOrderDtState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastOrderDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_order_dt", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<TradeOrderBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();
                        Long ts = DateFormatUtil.toTs(value.getString("create_time"), true);
                        String orderDt = DateFormatUtil.toDate(ts);
                        if (lastOrderDt == null) {
                            // 首次下单
                            out.collect(new TradeOrderBean("", "", 1L, 1L, ts));
                            lastOrderDtState.update(orderDt);
                        } else if (!lastOrderDt.equals(orderDt)) {
                            // 独立下单
                            out.collect(new TradeOrderBean("", "", 1L, 0L, ts));
                            lastOrderDtState.update(orderDt);
                        }
                    }
                });

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<TradeOrderBean> beanWithWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeOrderBean>() {
            @Override
            public long extractTimestamp(TradeOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        SingleOutputStreamOperator<TradeOrderBean> reduceStream = beanWithWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        return value1;
                    }
                }, new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        TradeOrderBean tradeOrderBean = values.iterator().next();
                        tradeOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeOrderBean.setTs(System.currentTimeMillis());
                        out.collect(tradeOrderBean);
                    }
                });

        // TODO 7 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_trade_order_window values(?,?,?,?,?)"));

        // TODO 8 执行任务
        env.execute(groupID);
    }
}
