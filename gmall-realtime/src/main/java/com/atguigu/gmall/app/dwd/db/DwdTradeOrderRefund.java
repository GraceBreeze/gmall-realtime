package com.atguigu.gmall.app.dwd.db;

import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class DwdTradeOrderRefund {
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

        // 设置TTL时间
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.state.ttl", "5 s");

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` map<string, string>,\n" +
                "`pt` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund"));

        // TODO 4. 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['refund_type'] refund_type,\n" +
                "data['refund_num'] refund_num,\n" +
                "data['refund_amount'] refund_amount,\n" +
                "data['refund_reason_type'] refund_reason_type,\n" +
                "data['refund_reason_txt'] refund_reason_txt,\n" +
                "data['create_time'] create_time,\n" +
                "pt,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'order_refund_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 5. 读取订单表数据，筛选退单数据
        Table orderInfoRefund = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "`old`\n" +
                "from topic_db\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and data['order_status']='1005'\n" +
                "and `old`['order_status'] is not null");

        tableEnv.createTemporaryView("order_info_refund", orderInfoRefund);

        // TODO 6. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicDDL());

        // TODO 7. 关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "ri.id,\n" +
                "ri.user_id,\n" +
                "ri.order_id,\n" +
                "ri.sku_id,\n" +
                "oi.province_id,\n" +
                "ri.create_time,\n" +
                "ri.refund_type,\n" +
                "type_dic.dic_name,\n" +
                "ri.refund_reason_type,\n" +
                "reason_dic.dic_name,\n" +
                "ri.refund_reason_txt,\n" +
                "ri.refund_num,\n" +
                "ri.refund_amount,\n" +
                "ri.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_refund_info ri\n" +
                "join \n" +
                "order_info_refund oi\n" +
                "on ri.order_id = oi.id\n" +
                "join \n" +
                "base_dic for system_time as of ri.pt as type_dic\n" +
                "on ri.refund_type = type_dic.dic_code\n" +
                "join\n" +
                "base_dic for system_time as of ri.pt as reason_dic\n" +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8. 建立 Kafka-Connector dwd_trade_order_refund 表
        tableEnv.executeSql("create table dwd_trade_order_refund(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "create_time string,\n" +
                "refund_type_code string,\n" +
                "refund_type_name string,\n" +
                "refund_reason_type_code string,\n" +
                "refund_reason_type_name string,\n" +
                "refund_reason_txt string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_order_refund"));

        // TODO 9. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_refund select * from result_table");
    }

}
