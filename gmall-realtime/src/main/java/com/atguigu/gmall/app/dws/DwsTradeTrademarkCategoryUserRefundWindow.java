package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.app.func.DimAsyncFunction;
import com.atguigu.gmall.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2022-09-01 10:23
 */
public class DwsTradeTrademarkCategoryUserRefundWindow {
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

        // TODO 3 从退单事实表中读取数据"dwd_trade_order_refund"
        String topicName = "dwd_trade_order_refund";
        String groupId = "dws_trade_trademark_category_user_refund_window";
        DataStreamSource<String> refundStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupId));

        // TODO 4 转换为对应的javaBean
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = refundStream.map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public TradeTrademarkCategoryUserRefundBean map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                HashSet<String> hashSet = new HashSet<>();
                hashSet.add(jsonObject.getString("order_id"));
                return TradeTrademarkCategoryUserRefundBean.builder()
                        .skuId(jsonObject.getString("sku_id"))
                        .orderIdSet(hashSet)
                        .refundCount(1L)
                        .userId(jsonObject.getString("user_id"))
                        .ts(Long.parseLong(jsonObject.getString("ts")) * 1000L)
                        .build();
            }
        });

        // TODO 5 维度关联SKU_INFO获取对应的字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> skuBeanStream = AsyncDataStream.unorderedWait(beanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory3Id(obj.getString("category3Id"));
                input.setTrademarkId(obj.getString("tmId"));
            }
        }, 100L, TimeUnit.SECONDS);

        // TODO 6 按照品牌品类用户进行聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanWithWatermarkStream = skuBeanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceStream = beanWithWatermarkStream.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, String>() {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean value) throws Exception {
                return value.getUserId() + value.getTrademarkId() + value.getCategory3Id();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1, TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                        Set<String> orderIdSet1 = value1.getOrderIdSet();
                        Set<String> orderIdSet2 = value2.getOrderIdSet();
                        orderIdSet1.addAll(orderIdSet2);
//                        value1.setOrderIdSet(orderIdSet1);
                        value1.setRefundCount((long) orderIdSet1.size());
                        return value1;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeTrademarkCategoryUserRefundBean> input, Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                        TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean = input.iterator().next();
                        tradeTrademarkCategoryUserRefundBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeTrademarkCategoryUserRefundBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeTrademarkCategoryUserRefundBean.setTs(System.currentTimeMillis());
                        out.collect(tradeTrademarkCategoryUserRefundBean);
                    }
                });
//        reduceStream.print("reduce>>>>>>");

        // TODO 7 单独关联品牌品类表格补全字段
        // 关联tm表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmBeanStream = AsyncDataStream.unorderedWait(reduceStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_trademark".toUpperCase()) {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setTrademarkName(obj.getString("tmName"));
            }
        }, 100L, TimeUnit.SECONDS);

        // 关联3级标签
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category3BeanStream = AsyncDataStream.unorderedWait(tmBeanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category3".toUpperCase()) {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory3Name(obj.getString("name"));
                input.setCategory2Id(obj.getString("category2Id"));
            }
        }, 100L, TimeUnit.SECONDS);

        // 关联2级标签
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category2BeanStream = AsyncDataStream.unorderedWait(category3BeanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category2".toUpperCase()) {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory2Name(obj.getString("name"));
                input.setCategory1Id(obj.getString("category1Id"));
            }
        }, 100L, TimeUnit.SECONDS);

        // 关联1级标签
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> category1BeanStream = AsyncDataStream.unorderedWait(category2BeanStream, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("dim_base_category1".toUpperCase()) {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject obj) {
                input.setCategory1Name(obj.getString("name"));
            }
        }, 100L, TimeUnit.SECONDS);
//        category1BeanStream.print("1>>>>");

        // TODO 8 写出到clickHouse中
        category1BeanStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?," +
        "?,?,?,?,?," +
         "?,?,?)"));

        // TODO 9 执行任务
        env.execute(groupId);
    }
}
