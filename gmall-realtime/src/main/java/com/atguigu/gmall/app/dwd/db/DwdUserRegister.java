package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdUserRegister {
    public static void main(String[] args) {
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

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_detail"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] user_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts \n" +
                "from topic_db \n" +
                "where `table` = 'user_info' \n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Kafka-Connector dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`(\n" +
                "`user_id` string,\n" +
                "`create_time` string,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_user_register\n" +
                "select \n" +
                "user_id,\n" +
                "create_time,\n" +
                "ts \n" +
                "from user_info");

    }
}

