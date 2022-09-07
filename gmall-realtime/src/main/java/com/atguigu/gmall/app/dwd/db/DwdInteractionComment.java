package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwdInteractionComment {
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
                "`pt` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_interaction_comment"));

        // TODO 4. 读取评论表数据
        Table commentInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['create_time'] create_time,\n" +
                "data['appraise'] appraise,\n" +
                "pt,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'comment_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("comment_info", commentInfo);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicDDL());

        // TODO 6. 关联两张表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "ci.id,\n" +
                "ci.user_id,\n" +
                "ci.sku_id,\n" +
                "ci.order_id,\n" +
                "ci.create_time,\n" +
                "ci.appraise,\n" +
                "dic.dic_name,\n" +
                "ts\n" +
                "from comment_info ci\n" +
                "join\n" +
                "base_dic for system_time as of ci.pt as dic\n" +
                "on ci.appraise = dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 建立 Kafka-Connector dwd_interaction_comment 表
        tableEnv.executeSql("create table dwd_interaction_comment(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "order_id string,\n" +
                "create_time string,\n" +
                "appraise_code string,\n" +
                "appraise_name string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_interaction_comment"));

        // TODO 8. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_interaction_comment select * from result_table");

    }
}

