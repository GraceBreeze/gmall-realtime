package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yhm
 * @create 2022-08-24 14:25
 */
public class DwsTrafficPageViewWindow {
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

        // TODO 3 读取kafka的page_log数据
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dws_traffic_page_view_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupID));

        // TODO 4 过滤首页和详情页的访问数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String pageID = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageID) || "good_detail".equals(pageID)) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 独立访客去重
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanStream = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            ValueState<String> homeLastVisitDt = null;
            ValueState<String> detailLastVisitDt = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 状态的存活时间 对程序的内存进行一定的优化
                ValueStateDescriptor<String> homeLastVisitDtDescriptor = new ValueStateDescriptor<>("home_last_visit_dt", String.class);
                homeLastVisitDtDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                this.homeLastVisitDt = getRuntimeContext().getState(homeLastVisitDtDescriptor);

                ValueStateDescriptor<String> detailLastVisitDtDescriptor = new ValueStateDescriptor<>("detail_last_visit_dt", String.class);
                detailLastVisitDtDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                this.detailLastVisitDt = getRuntimeContext().getState(detailLastVisitDtDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                // 判断数据为首页还是详情页
                String pageID = value.getJSONObject("page").getString("page_id");
                String visitDt = DateFormatUtil.toDate(value.getLong("ts"));
                if ("home".equals(pageID)) {
                    String homeLastVisit = homeLastVisitDt.value();
                    if (homeLastVisit == null || !homeLastVisit.equals(visitDt)) {
                        // 独立访客
                        homeLastVisitDt.update(visitDt);
                        out.collect(new TrafficHomeDetailPageViewBean("", "", 1L, 0L, value.getLong("ts")));
                    }
                } else {
                    // 详情数据
                    String detailLastVisit = detailLastVisitDt.value();
                    if (detailLastVisit == null || !detailLastVisit.equals(visitDt)) {
                        detailLastVisitDt.update(visitDt);
                        out.collect(new TrafficHomeDetailPageViewBean("", "", 0L, 1L, value.getLong("ts")));
                    }
                }
            }
        });

        // TODO 6 开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanWithWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficHomeDetailPageViewBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowStream = beanWithWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceStream = windowStream.reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                return value1;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                TrafficHomeDetailPageViewBean bean = values.iterator().next();
                bean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                bean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                bean.setTs(System.currentTimeMillis());
                out.collect(bean);
            }
        });

        // TODO 7 写出到clickHouse中
        reduceStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        // TODO 8 执行任务
        env.execute(groupID);
    }
}
