package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.app.func.DimAsyncFunction;
import com.atguigu.gmall.bean.TradeProvinceOrderWindow;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2022-09-01 9:05
 */
public class DwsTradeProvinceOrderWindow {
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

        // TODO 3 读取topic_db中的数据
        String topicName = "topic_db";
        String groupId = "dws_trade_province_order_window";
        DataStreamSource<String> dbStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupId));

        // TODO 4 过滤出下单数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = dbStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String table = jsonObject.getString("table");
                    String type = jsonObject.getString("type");
                    JSONObject data = jsonObject.getJSONObject("data");
                    if ("order_info".equals(table) && "insert".equals(type)) {
                        out.collect(data);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        // TODO 5 转换结构为javaBean
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanStream = jsonObjStream.map(new MapFunction<JSONObject, TradeProvinceOrderWindow>() {
            @Override
            public TradeProvinceOrderWindow map(JSONObject value) throws Exception {
                String provinceId = value.getString("province_id");
                Double totalAmount = value.getDouble("total_amount");
                Long ts = DateFormatUtil.toTs(value.getString("create_time"), true);
                return TradeProvinceOrderWindow.builder()
                        .provinceId(provinceId)
                        .orderAmount(totalAmount)
                        .orderCount(1L)
                        .ts(ts)
                        .build();
            }
        });

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> beanWithWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeProvinceOrderWindow>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
            @Override
            public long extractTimestamp(TradeProvinceOrderWindow element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        KeyedStream<TradeProvinceOrderWindow, String> keyedStream = beanWithWatermarkStream.keyBy(new KeySelector<TradeProvinceOrderWindow, String>() {
            @Override
            public String getKey(TradeProvinceOrderWindow value) throws Exception {
                return value.getProvinceId();
            }
        });

        SingleOutputStreamOperator<TradeProvinceOrderWindow> reduceStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow value1, TradeProvinceOrderWindow value2) throws Exception {
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        return value1;
                    }
                }, new WindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderWindow> input, Collector<TradeProvinceOrderWindow> out) throws Exception {
                        TradeProvinceOrderWindow tradeProvinceOrderWindow = input.iterator().next();
                        tradeProvinceOrderWindow.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeProvinceOrderWindow.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeProvinceOrderWindow.setTs(System.currentTimeMillis());
                        out.collect(tradeProvinceOrderWindow);
                    }
                });


        // TODO 7 关联维度表省份表
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeProvinceOrderWindow>("dim_base_province".toUpperCase()) {
            @Override
            public String getKey(TradeProvinceOrderWindow input) {
                return input.getProvinceId();
            }

            @Override
            public void join(TradeProvinceOrderWindow input, JSONObject obj) {
                input.setProvinceName(obj.getString("name"));
            }
        }, 100L, TimeUnit.SECONDS);



        // TODO 8 写出到clickHouse中
        resultStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_trade_province_order_window values(?,?,?,?,?,?,?)"));

         // TODO 9 执行任务
        env.execute(groupId);
    }
}
