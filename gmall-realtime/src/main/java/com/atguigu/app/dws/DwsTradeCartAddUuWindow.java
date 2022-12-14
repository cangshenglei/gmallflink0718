package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.CartAddUuBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdTradeCartAdd -> Kafka(ZK) -> DwsTradeCartAddUuWindow -> ClickHouse(ZK)
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {

        //todo 1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //状态后端设置
        //        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(
        //                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        //        );
        //        env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        //        ));
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //todo 2,读取KAFKA dwd层 加购主题数据创建流
        String topic = "dwd_trade_cart_add";
        String groupId = "cart_add_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //todo 3,将Strin转为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //todo  4,提取时间戳生成Wtermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String operateTime = jsonObject.getString("operate_time");
                if (operateTime != null) {
                    return DateFormatUtil.toTs(operateTime, true);
                } else {
                    return DateFormatUtil.toTs(jsonObject.getString("create_time"), true);
                }
            }
        }));
        //todo   5,依据UserID分组

        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));
        //todo  6,去重数据并转化数据为JavaBean对象
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-cart", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastCartDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<CartAddUuBean> collector) throws Exception {
                //获取状态当前数据的日期
                String lastDt = lastCartDtState.value();
                String curDt = "";
                String operateTime = jsonObject.getString("operate_time");

                if (operateTime != null) {
                    curDt = operateTime.split(" ")[0];
                } else {
                    curDt = jsonObject.getString("create_time").split(" ")[0];
                }
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartDtState.update(curDt);
                    collector.collect(new CartAddUuBean("", "", 1L, null));
                }
                //
            }
        });

        //todo  7,开窗,聚合
        SingleOutputStreamOperator<CartAddUuBean> reduceDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean cartAddUuBean, CartAddUuBean t1) throws Exception {
                        cartAddUuBean.setCartAddUuCt(cartAddUuBean.getCartAddUuCt() + t1.getCartAddUuCt());
                        return cartAddUuBean;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CartAddUuBean> iterable, Collector<CartAddUuBean> collector) throws Exception {
                        CartAddUuBean next = iterable.iterator().next();

                        next.setTs(System.currentTimeMillis());
                        next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        collector.collect(next);
                    }
                });

        //todo  8,将数据写出到CLICKHouse
        reduceDS.print(">>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values(?,?,?,?)"));

        //TODO 9,启动任务
        env.execute("DwsTradeCartAddUuWindow");

    }
}
