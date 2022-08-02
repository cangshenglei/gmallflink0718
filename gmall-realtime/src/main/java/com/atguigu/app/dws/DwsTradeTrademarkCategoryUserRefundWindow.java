package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple3;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeOrderRefund -> Kafka(ZK) -> DwsTradeTrademarkCategoryUserRefundWindow(Redis,Phoenix) -> ClickHouse(ZK)
public class DwsTradeTrademarkCategoryUserRefundWindow {
    public static void main(String[] args) throws Exception {

        //todo 1,环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(
//                "hdfs://hadoop102:8020/ck"
//        );
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //todo 2,从kafak_dwd_trade+order_refund主题读取退单明细表
        String pageTopic = "dwd_trade_order_refund";
        String groupId = "trademark_category_user_refund_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));
        //TODO 3,将数据转化为JAVAbEAN
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkCategoryUserRefundDS = kafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            return TradeTrademarkCategoryUserRefundBean.builder()
                    .userId(jsonObject.getString("user_id"))
                    .skuId(jsonObject.getString("sku_id"))
                    .refundAmount(jsonObject.getDouble("refund_amount"))
                    .refundCount(1L)
                    .ts(DateFormatUtil.toTs(jsonObject.getString("create_time"), true))
                    .build();

        });

        //TODO 4,设置时间水位线,生成watermarks 关联维度表,补充相关的维度字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tradeTrademarkCategoryUserRefundWithWmDS = tradeTrademarkCategoryUserRefundDS.assignTimestampsAndWatermarks(WatermarkStrategy.<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserRefundBean>() {
            @Override
            public long extractTimestamp(TradeTrademarkCategoryUserRefundBean element, long recordTimestamp) {
                return element.getTs();
            }
        }));

        //TODO 5,关联SKU_INFO维度表,补充相关的维度字段
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> refundOrderWithSkuDS = AsyncDataStream.unorderedWait(tradeTrademarkCategoryUserRefundDS, new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_SKU_INFO") {
            @Override
            public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                return input.getSkuId();
            }

            @Override
            public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                input.setCategory3Id(dimInfo.getString("CATEGORY3_ID"));
                input.setTrademarkId(dimInfo.getString("TM_ID"));
            }
        }, 100, TimeUnit.SECONDS);

        //TODO 6,分组,开窗,聚合
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reduceDS = refundOrderWithSkuDS.keyBy(new KeySelector<TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> getKey(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean) throws Exception {
                return new Tuple3<>(tradeTrademarkCategoryUserRefundBean.getUserId(), tradeTrademarkCategoryUserRefundBean.getTrademarkId(), tradeTrademarkCategoryUserRefundBean.getCategory3Id());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean tradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean t1) throws Exception {
                        tradeTrademarkCategoryUserRefundBean.setRefundCount(tradeTrademarkCategoryUserRefundBean.getRefundCount() + t1.getRefundCount());
                        tradeTrademarkCategoryUserRefundBean.setRefundAmount(tradeTrademarkCategoryUserRefundBean.getRefundAmount() + t1.getRefundAmount());
                        return tradeTrademarkCategoryUserRefundBean;
                    }
                }, new WindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple3<String, String, String> stringStringStringTuple3, TimeWindow timeWindow, Iterable<TradeTrademarkCategoryUserRefundBean> iterable, Collector<TradeTrademarkCategoryUserRefundBean> collector) throws Exception {
                        //获取数据
                        TradeTrademarkCategoryUserRefundBean next = iterable.iterator().next();
                        //补充信息
                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));

                        //输出数据
                        collector.collect(next);
                    }
                });

        //TODO 7,维度表关联,补充相关的维度字段
        //TM
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> refundOrderWithTmDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setTrademarkName(dimInfo.getString("TM_NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //Category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> refundOrderWithCategory3DS = AsyncDataStream.unorderedWait(
                refundOrderWithTmDS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory3Name(dimInfo.getString("NAME"));
                        input.setCategory2Id(dimInfo.getString("CATEGORY2_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        //Category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> refundOrderWithCategory2DS = AsyncDataStream.unorderedWait(
                refundOrderWithCategory3DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory2Name(dimInfo.getString("NAME"));
                        input.setCategory1Id(dimInfo.getString("CATEGORY1_ID"));
                    }
                }, 100, TimeUnit.SECONDS);

        //Category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> refundOrderWithCategory1DS = AsyncDataStream.unorderedWait(
                refundOrderWithCategory2DS,
                new DimAsyncFunction<TradeTrademarkCategoryUserRefundBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTrademarkCategoryUserRefundBean input) {
                        return input.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTrademarkCategoryUserRefundBean input, JSONObject dimInfo) {
                        input.setCategory1Name(dimInfo.getString("NAME"));
                    }
                }, 100, TimeUnit.SECONDS);

        //TODO 8.将数据写出到ClickHouse
        refundOrderWithCategory1DS.print(">>>>>>>>>>>>");
        refundOrderWithCategory1DS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_trademark_category_user_refund_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("DwsTradeTrademarkCategoryUserRefundWindow");
    }
}
