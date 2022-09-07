package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.CartAddUuBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-26 14:05
 */
public class DwsTradeCartAddUuWindow {
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

        // TODO 3 kafka加购主题数据
        String topicName = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> cartAddStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupId));

        // TODO 4 转化结构为json
        SingleOutputStreamOperator<JSONObject> jsonObjStream = cartAddStream.map(JSON::parseObject);

        // TODO 5 根据状态去重得到独立加购人数
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(jsonObj -> jsonObj.getString("user_id"));

        SingleOutputStreamOperator<CartAddUuBean> beanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            ValueState<String> lastAddDtState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastAddDtDescriptor = new ValueStateDescriptor<>("last_add_dt", String.class);
                lastAddDtDescriptor.enableTimeToLive(StateTtlConfig
                        .newBuilder(Time.days(1L))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                lastAddDtState = getRuntimeContext().getState(lastAddDtDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {
                Long ts = value.getLong("ts");
                String cartAddDt = DateFormatUtil.toDate(ts);
                String lastAddDt = lastAddDtState.value();
                if (lastAddDt == null || !lastAddDt.equals(cartAddDt)) {
                    // 独立登录
                    out.collect(new CartAddUuBean("", "", 1L, ts * 1000L));
                    lastAddDtState.update(cartAddDt);
                }
            }
        });

        // TODO 6 开窗聚合数据
        SingleOutputStreamOperator<CartAddUuBean> beanWithWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<CartAddUuBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<CartAddUuBean>() {
            @Override
            public long extractTimestamp(CartAddUuBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        SingleOutputStreamOperator<CartAddUuBean> reduceStream = beanWithWatermarkStream.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean cartAddUuBean = values.iterator().next();
                        cartAddUuBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        cartAddUuBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        cartAddUuBean.setTs(System.currentTimeMillis());
                        out.collect(cartAddUuBean);
                    }
                });


        // TODO 7 写出到clickHouse中
        reduceStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        // TODO 8 执行任务
        env.execute(groupId);

    }
}
