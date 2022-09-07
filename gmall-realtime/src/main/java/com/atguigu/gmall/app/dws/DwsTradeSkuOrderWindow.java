package com.atguigu.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.app.func.DimAsyncFunction;
import com.atguigu.gmall.bean.TradeSkuOrderBean;
import com.atguigu.gmall.util.ClickHouseUtil;
import com.atguigu.gmall.util.DateFormatUtil;
import com.atguigu.gmall.util.KafkaUtil;
import com.atguigu.gmall.util.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author yhm
 * @create 2022-08-27 9:22
 */
public class DwsTradeSkuOrderWindow {
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

        // TODO 3 读取kafka对应主题的数据dwd_trade_order_detail
        String topicName = "dwd_trade_order_detail";
        String groupID = "dws_trade_sku_order_window";
        DataStreamSource<String> orderDetailStream = env.addSource(KafkaUtil.getKafkaConsumer(topicName, groupID));

        // TODO 4 转换结构
        // TODO 5 过滤掉不完整的数据
        SingleOutputStreamOperator<JSONObject> jsonObjStream = orderDetailStream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String userId = jsonObject.getString("user_id");
                String sourceType = jsonObject.getString("source_type");
                if (userId != null && sourceType != null) {
                    out.collect(jsonObject);
                }
            }
        });

        // TODO 6 按照订单详情分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getString("id");
            }
        });

        // TODO 7 去重因为left join造成的重复数据  (撤回流发送的null数据已经被上一级过滤掉)
        SingleOutputStreamOperator<JSONObject> processStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> lastOrderJsonObjState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastOrderJsonObjState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("last_order_jsonObj", JSONObject.class));
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject lastOrderJsonObj = lastOrderJsonObjState.value();
                if (lastOrderJsonObj == null) {
                    // 当前id的第一条数据
                    lastOrderJsonObjState.update(value);
                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                } else {
                    // 后续的数据
                    // 比较join的时间  2022-08-26 07:47:30.513Z
                    String rowOpTs = value.getString("row_op_ts");
                    String lastRowOpTs = lastOrderJsonObj.getString("row_op_ts");
                    if (TimestampLtz3CompareUtil.compare(rowOpTs,lastRowOpTs) >= 0) {
                        lastOrderJsonObjState.update(value);
                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                // 输出状态中的值
                JSONObject lastOrderJsonObj = lastOrderJsonObjState.value();
                out.collect(lastOrderJsonObj);
                lastOrderJsonObjState.clear();
            }
        });

        // TODO 8 根据sku来聚合之后再开窗累加
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = processStream.map(new MapFunction<JSONObject, TradeSkuOrderBean>() {
            @Override
            public TradeSkuOrderBean map(JSONObject value) throws Exception {
                return TradeSkuOrderBean.builder()
                        .skuId(value.getString("sku_id"))
                        .skuName(value.getString("sku_name"))
                        .originalAmount(value.getDouble("split_original_amount") == null ? 0.0: value.getDouble("split_original_amount"))
                        .activityAmount(value.getDouble("split_activity_amount") == null ? 0.0 : value.getDouble("split_activity_amount"))
                        .couponAmount(value.getDouble("split_coupon_amount") == null ? 0.0 : value.getDouble("split_coupon_amount"))
                        .orderAmount(value.getDouble("split_total_amount") == null ? 0.0 : value.getDouble("split_total_amount"))
                        .ts(value.getLong("od_ts") * 1000L)
                        .build();
            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> beanWithWatermarkStream = beanStream.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2L)).withTimestampAssigner(new SerializableTimestampAssigner<TradeSkuOrderBean>() {
            @Override
            public long extractTimestamp(TradeSkuOrderBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        SingleOutputStreamOperator<TradeSkuOrderBean> reduceBeanStream = beanWithWatermarkStream.keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce(new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) throws Exception {
                        value1.setOriginalAmount(value1.getOriginalAmount() + value2.getOriginalAmount());
                        value1.setActivityAmount(value1.getActivityAmount() + value2.getActivityAmount());
                        value1.setCouponAmount(value1.getCouponAmount() + value2.getCouponAmount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        return value1;
                    }
                }, new WindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeSkuOrderBean> input, Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean tradeSkuOrderBean = input.iterator().next();
                        tradeSkuOrderBean.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        tradeSkuOrderBean.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        tradeSkuOrderBean.setTs(System.currentTimeMillis());
                        out.collect(tradeSkuOrderBean);
                    }
                });


        // TODO 9 和DIM层的维度表进行关联
        // 和 DIM_SKU_INFO 关联
        //{"skuName":"小米10 至尊纪念版 手机","tmId":"1","createTime":"2020-11-11 14:07:35","price":"59","category3Id":"61","weight":"100.00","skuDefaultImg":"http://47.93.148.192:8080/group1/M00/00/01/rBHu8l-rfvmAIpgZAAIvrX6L9fo612.jpg","isSale":"1","spuId":"1","skuDesc":"小米10 至尊纪念版 双模5G 骁龙865 120HZ高刷新率 120倍长焦镜头 120W快充 12GB+256GB 陶瓷黑 游戏手机","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> skuBeanStream = AsyncDataStream.unorderedWait(reduceBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_SKU_INFO") {

            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setSkuName(obj.getString("skuName"));
                input.setTrademarkId(obj.getString("tmId"));
                input.setSpuId(obj.getString("spuId"));
                input.setCategory3Id(obj.getString("category3Id"));
            }
        }, 100, TimeUnit.SECONDS);
        skuBeanStream.print("sku>>>>");

        // 关联spu
        // {"spuName":"小米10","tmId":"1","category3Id":"61","description":"小米10","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> spuBeanStream = AsyncDataStream.unorderedWait(skuBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_SPU_INFO") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getSpuId();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setSpuName(obj.getString("spuName"));

            }
        }, 100, TimeUnit.SECONDS);
        spuBeanStream.print("spu>>>>");

        // 关联品牌表
        //{"tmName":"三星","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> tmBeanStream = AsyncDataStream.unorderedWait(spuBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_TRADEMARK") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getTrademarkId();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setTrademarkName(obj.getString("tmName"));
            }
        }, 100, TimeUnit.SECONDS);
        tmBeanStream.print("tm>>>");

        // 关联3级标签
        //{"name":"电子书","category2Id":"1","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> category3BeanStream = AsyncDataStream.unorderedWait(tmBeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY3") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getCategory3Id();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setCategory3Name(obj.getString("name"));
                input.setCategory2Id(obj.getString("category2Id"));
            }
        }, 100, TimeUnit.SECONDS);
        category3BeanStream.print("c3>>>");

        // 关联2级标签
        //{"name":"电子书刊","category1Id":"1","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> category2BeanStream = AsyncDataStream.unorderedWait(category3BeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY2") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getCategory2Id();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setCategory2Name(obj.getString("name"));
                input.setCategory1Id(obj.getString("category1Id"));
            }
        }, 100, TimeUnit.SECONDS);
        category2BeanStream.print("c2>>>");

        // 关联1级标签
        //{"name":"图书、音像、电子书刊","id":"1"}
        SingleOutputStreamOperator<TradeSkuOrderBean> resultBeanStream = AsyncDataStream.unorderedWait(category2BeanStream, new DimAsyncFunction<TradeSkuOrderBean>("DIM_BASE_CATEGORY1") {
            @Override
            public String getKey(TradeSkuOrderBean input) {
                return input.getCategory1Id();
            }

            @Override
            public void join(TradeSkuOrderBean input, JSONObject obj) {
                input.setCategory1Name(obj.getString("name"));
            }
        }, 100, TimeUnit.SECONDS);

        resultBeanStream.print("result>>>");

        // TODO 10 写出数据到clickHouse中
        resultBeanStream.addSink(ClickHouseUtil.getClickHouseSinkFunc("insert into dws_trade_sku_order_window values(?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?,?," +
                "?,?,?,?)"));

        // TODO 11 执行任务
        env.execute();

    }
}
