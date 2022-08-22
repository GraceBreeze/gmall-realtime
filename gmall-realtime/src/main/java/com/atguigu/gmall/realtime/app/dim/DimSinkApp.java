package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author GraceBreeze
 * @create 2022-08-22 18:09
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        //TODO 1 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2 设置状态后端
          /*
                  env.enableCheckpointing(5 * 60 * 1000L, CheckpointingMode.EXACTLY_ONCE );
                  env.getCheckpointConfig().setCheckpointTimeout( 3 * 60 * 1000L );
                  env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
                  env.setStateBackend(new HashMapStateBackend());
                  env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
                  System.setProperty("HADOOP_USER_NAME", "atguigu");
           */

        //TODO 3 读取kafka的topic主题数据
        String topicName = "topic_db";
        String groupID = "dim_sink_app";
        DataStreamSource<String> topicDBStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        //TODO 4 改变数据结构并将脏数据写出到侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjStream = topicDBStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    String type = jsonObject.getString("type");
                    if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
                        ctx.output(dirtyTag, value);
                    } else {
                        out.collect(jsonObject);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    ctx.output(dirtyTag, value);
                }
            }
        });

        // 获取脏数据流
        DataStream<String> dirtyStream = jsonObjStream.getSideOutput(dirtyTag);
        dirtyStream.print("dirty>>>>>>>");

        // 执行任务
        env.execute("dim_sink_app");

    }


}