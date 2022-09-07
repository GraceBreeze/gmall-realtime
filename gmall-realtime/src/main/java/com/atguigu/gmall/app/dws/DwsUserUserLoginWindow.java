package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.UserLoginBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
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
 * @create 2022-08-26 9:18
 */
public class DwsUserUserLoginWindow {
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

        // TODO 3 读取kafka主题page_log数据
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dws_user_user_login_window";
        DataStreamSource<String> pageStream = env.addSource(KafkaUtil.getKafkaConsumer(page_topic, groupID));

        // TODO 4 过滤加转换用户登录数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                // 判断是否为登录用户数据
                if (common.getString("uid") != null && (page.getString("last_page_id") == null || page.getString("last_page_id").equals("login"))) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 5 添加水位线
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkStream = jsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        // TODO 6 按照uid分组
        // TODO 7 根据登录时间的状态去重
        SingleOutputStreamOperator<UserLoginBean> beanStream = jsonObjWithWatermarkStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"))
                .flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {

                    ValueState<String> lastLoginDtState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last_login_dt", String.class));
                    }

                    @Override
                    public void flatMap(JSONObject value, Collector<UserLoginBean> out) throws Exception {
                        String lastLoginDt = lastLoginDtState.value();
                        Long ts = value.getLong("ts");
                        String loginDt = DateFormatUtil.toDate(ts);
                        if (lastLoginDt == null || !lastLoginDt.equals(loginDt)) {
                            if (lastLoginDt != null && (ts - DateFormatUtil.toTs(lastLoginDt) > 1000L * 60 * 60 * 24 * 7)) {
                                // 7天回流
                                out.collect(new UserLoginBean("", "", 1L, 1L, ts));
                                lastLoginDtState.update(loginDt);
                            } else {
                                // 当日独立
                                out.collect(new UserLoginBean("", "", 0L, 1L, ts));
                                lastLoginDtState.update(loginDt);
                            }
                        }
                    }
                });


        // TODO 8 开窗聚合
        SingleOutputStreamOperator<UserLoginBean> reduceStream = beanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean userLoginBean = values.iterator().next();
                        userLoginBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userLoginBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        userLoginBean.setTs(System.currentTimeMillis());
                        out.collect(userLoginBean);
                    }
                });

        // TODO 9 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_user_user_login_window values(?,?,?,?,?)"));

        // TODO 10 执行任务
        env.execute(groupID);
    }
}
