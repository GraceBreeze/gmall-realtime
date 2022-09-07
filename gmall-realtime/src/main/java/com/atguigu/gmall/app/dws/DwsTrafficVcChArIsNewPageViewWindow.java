package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TrafficPageViewBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-24 10:24
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {
        // TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2 设置状态后端
        /*
        env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
        env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "atguigu");
         */

        // TODO 3 读取3条流的数据
        String page_topic = "dwd_traffic_page_log";
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        String ujTopic = "dwd_traffic_user_jump_detail";
        String groupID = "dws_traffic_vc_ch_ar_is_new_page_view_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupID));
        DataStreamSource<String> uvStream = env.addSource(KafkaUtil.getKafkaConsumer(uvTopic, groupID));
        DataStreamSource<String> ujStream = env.addSource(KafkaUtil.getKafkaConsumer(ujTopic, groupID));

        // TODO 4 转换为相同的机构javaBean
        SingleOutputStreamOperator<TrafficPageViewBean> uvBeanStream = uvStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"),
                        common.getString("ar"), common.getString("is_new"), 1L, 0L, 0L, 0L, 0L, jsonObject.getLong("ts"));
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> ujBeanStream = ujStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"),
                        common.getString("ar"), common.getString("is_new"), 0L, 0L, 0L, 0L, 1L, jsonObject.getLong("ts"));
            }
        });

        SingleOutputStreamOperator<TrafficPageViewBean> pageBeanStream = pageStream.map(new MapFunction<String, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                long sv = 0;
                if (page.getString("last_page_id") == null) {
                    sv = 1;
                }
                return new TrafficPageViewBean("", "", common.getString("vc"), common.getString("ch"),
                        common.getString("ar"), common.getString("is_new"), 0L, sv, 1L, page.getLong("during_time"), 0L, jsonObject.getLong("ts"));
            }
        });

        // TODO 5 union连接3条流的数据
        DataStream<TrafficPageViewBean> unionStream = pageBeanStream.union(ujBeanStream).union(uvBeanStream);

        // TODO 6 设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> beanWithWatermarkStream = unionStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(15)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        // TODO 7 开窗聚合数据
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedStream = beanWithWatermarkStream.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean value) throws Exception {
                return new Tuple4<>(value.getVc(), value.getCh(), value.getAr(), value.getIsNew());
            }
        });

        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10))).allowedLateness(Time.seconds(2));

        SingleOutputStreamOperator<TrafficPageViewBean> reduceStream = windowStream.reduce(new ReduceFunction<TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                return value1;
            }
        }, new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> out) throws Exception {
                TrafficPageViewBean trafficPageViewBean = input.iterator().next();
                trafficPageViewBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                trafficPageViewBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                trafficPageViewBean.setTs(System.currentTimeMillis());
                out.collect(trafficPageViewBean);
            }
        });

        reduceStream.print(">>>");
        // TODO 8 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.<TrafficPageViewBean>getClickHouseSinkFunc("insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9 执行任务
        env.execute(groupID);
    }
}
