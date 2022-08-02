package com.atguigu.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> nginx -> 日志服务器(File) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock_log -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficPageViewWindow -> ClickHouse(ZK)
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {
        //TODO 1,获取执行环境
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
        //TODO 2,读取dwd层的页面日志主题数据创建流
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "page_view_uv_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));
        //TODO 3,将数据转为JSON对象,并过滤出需要的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    collector.collect(jsonObject);
                }
            }
        });
        //todo 4,提取时间戳生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        //todo 5,按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //TODO 6,去重数据并转化为JavaBean对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> homeDetailPageViewDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeState;
            private ValueState<String> detailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ValueStateDescriptor<String> homeDesc = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailDesc = new ValueStateDescriptor<>("detail-state", String.class);

                //设置TTL  过期时间为1天
                homeDesc.enableTimeToLive(ttlConfig);
                detailDesc.enableTimeToLive(ttlConfig);

                homeState = getRuntimeContext().getState(homeDesc);
                detailState = getRuntimeContext().getState(detailDesc);


            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取状态日期以及当前数据中的日期以及当前的页面ID
                String homeLastDt = homeState.value();
                String detailStateDt = detailState.value();
                String curDt = DateFormatUtil.toDate(jsonObject.getLong("ts"));
                String pageId = jsonObject.getJSONObject("page").getString("page_id");

                long homeUvCt = 0L;
                long goodDetailUvCt = 0L;

                if ("home".equals(pageId) && (homeLastDt == null || !homeLastDt.equals(curDt))) {
                    homeUvCt = 1L;
                    homeState.update(curDt);
                } else if ("good_detail".equals(pageId) && (detailStateDt == null || !detailStateDt.equals(curDt))) {
                    goodDetailUvCt = 1L;
                    detailState.update(curDt);
                }

                //输出数据
                if (homeUvCt == 1L || goodDetailUvCt == 1L) {
                    collector.collect(new TrafficHomeDetailPageViewBean("", "", homeUvCt, goodDetailUvCt, null));
                }


            }
        });

        //TODO 7,开窗,聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = homeDetailPageViewDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10))).reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
            @Override
            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean t1) throws Exception {
                trafficHomeDetailPageViewBean.setHomeUvCt(trafficHomeDetailPageViewBean.getHomeUvCt() + t1.getHomeUvCt());
                trafficHomeDetailPageViewBean.setGoodDetailUvCt(trafficHomeDetailPageViewBean.getGoodDetailUvCt() + t1.getGoodDetailUvCt());
                return trafficHomeDetailPageViewBean;
            }
        }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                //获取数据
                TrafficHomeDetailPageViewBean next = iterable.iterator().next();

                //补充详细信息
                next.setTs(System.currentTimeMillis());
                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));


                //输出数据
                collector.collect(next);
            }
        });


        //TODO 8,将数据写入到Clickhouse
        reduceDS.print(">>>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));
        //TODO 9,启动任务
        env.execute("DwsTrafficPageViewWindow");


    }
}
