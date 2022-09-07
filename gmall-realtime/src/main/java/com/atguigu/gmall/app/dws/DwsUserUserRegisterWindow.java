package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.bean.UserRegisterBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @create 2022-08-26 10:27
 */
public class DwsUserUserRegisterWindow {
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

        // TODO 3 读取DWD层注册数据
        String topicName = "dwd_user_register";
        String groupID = "dws_user_user_register_window";
        DataStreamSource<String> registerStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));


        // TODO 4 转换数据结构
        SingleOutputStreamOperator<UserRegisterBean> beanStream = registerStream.map(new MapFunction<String, UserRegisterBean>() {
            @Override
            public UserRegisterBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return new UserRegisterBean("", "", 1L, jsonObject.getLong("ts") * 1000L);
            }
        });

        // TODO 5 开窗聚合
        SingleOutputStreamOperator<UserRegisterBean> beanWithWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        SingleOutputStreamOperator<UserRegisterBean> reduceStream = beanWithWatermarkStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                    @Override
                    public UserRegisterBean reduce(UserRegisterBean value1, UserRegisterBean value2) throws Exception {
                        value1.setRegisterCt(value1.getRegisterCt() + value2.getRegisterCt());
                        return value1;
                    }
                }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserRegisterBean> values, Collector<UserRegisterBean> out) throws Exception {
                        UserRegisterBean userRegisterBean = values.iterator().next();
                        userRegisterBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        userRegisterBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        userRegisterBean.setTs(System.currentTimeMillis());
                        out.collect(userRegisterBean);
                    }
                });

        // TODO 6 写出到clickHouse
        reduceStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_user_user_register_window values(?,?,?,?)"));

        // TODO 7 执行任务
        env.execute(groupID);

    }
}
