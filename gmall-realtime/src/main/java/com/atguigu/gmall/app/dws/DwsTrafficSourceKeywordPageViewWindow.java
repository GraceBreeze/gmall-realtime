package com.atguigu.gmall.app.dws;

import com.atguigu.gmall.app.func.KeywordUDTF;
import com.atguigu.gmall.bean.KeywordBean;
import com.atguigu.gmall.common.GmallConstant;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2022-08-23 11:32
 */
public class DwsTrafficSourceKeywordPageViewWindow {
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

        // TODO 3 使用sql的形式读取数据dwd_traffic_page_log
        // {"common":{"ar":"110000","uid":"840","os":"iOS 13.2.9","ch":"Appstore","is_new":"0","md":"iPhone Xs Max","mid":"mid_855298","vc":"v2.1.111","ba":"iPhone"},"page":{"page_id":"trade","item":"8","during_time":13561,"item_type":"sku_ids","last_page_id":"cart"},"ts":1645452180000}
        String page_topic = "dwd_traffic_page_log";
        String groupID = "dws_traffic_source_keyword_page_view_window";
        tableEnv.executeSql("create table page_log(\n" +
                "   `common` map<string,string>,\n" +
                "   `page` map<string,string>,\n" +
                "   `ts` bigint, \n" +
                "    rt AS TO_TIMESTAMP_LTZ(ts, 3),  \n" +
                "   WATERMARK FOR  rt AS rt - INTERVAL '2' SECOND " +
                ")" + KafkaUtil.getKafkaDDL(page_topic,groupID));

        // TODO 4 过滤出关键词数据
        Table filterTable = tableEnv.sqlQuery("select \n" +
                "   `page`['item'] keyword,\n" +
                "    rt \n" +
                "from page_log\n" +
                "where `page`['item_type']='keyword'\n" +
                "and `page`['last_page_id']='search'\n" +
                "and `page`['item'] is not null");

        tableEnv.createTemporaryView("filter_table",filterTable);


        // TODO 5 调用拆词的函数对keyword进行拆分
        // 注册需要使用的拆分函数
        tableEnv.createTemporaryFunction("analyze_keyword", KeywordUDTF.class);

        Table wordTable = tableEnv.sqlQuery("select \n" +
                "    rt ,  \n" +
                "  word\n" +
                "FROM filter_table,\n" +
                "LATERAL TABLE(analyze_keyword(keyword))");
        tableEnv.createTemporaryView("word_table",wordTable);

        // TODO 6 开窗聚合词语
        Table countTable = tableEnv.sqlQuery("select \n" +
                "  DATE_FORMAT(TUMBLE_START(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "  DATE_FORMAT(TUMBLE_END(rt, interval '10' second), 'yyyy-MM-dd HH:mm:ss')  edt,\n" +
                "  word keyword,\n" +
                "  '123' err,\n" +
                "  count(*) keyword_count, \n " +
                "'" +  GmallConstant.KEYWORD_SEARCH + "'  source , \n " +
                "  UNIX_TIMESTAMP()*1000 ts \n " +
                "from word_table\n" +
                "group by word,\n" +
                "TUMBLE(rt, INTERVAL '10' SECOND)");

        // TODO 7 转换数据为流
        DataStream<KeywordBean> beanDataStream = tableEnv.toAppendStream(countTable, KeywordBean.class);


        // TODO 8 写出到clickHouse中
        String sql = "insert into dws_traffic_source_keyword_page_view_window values(?,?,?,?,?,?)";
        beanDataStream.addSink(ClickHouseUtil.<KeywordBean>getClickHouseSinkFunc(sql));


        env.execute(groupID);


    }
}
