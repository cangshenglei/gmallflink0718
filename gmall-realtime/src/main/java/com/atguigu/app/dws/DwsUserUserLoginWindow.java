package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserLoginBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> nginx -> 日志服务器(File) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock_log -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsUserUserLoginWindow -> ClickHouse(ZK)
public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1,获取执行环境
        //TODO 1.获取执行环境
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

        //TODO  2,读取kafak DWD 层页面日志主题数据创建流
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "user_login_220212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));
        //TODO   3,转换为JSON对象并过滤数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                if (uid != null && (lastPageId == null || "login".equals(lastPageId))) {
                    collector.collect(jsonObject);
                }
            }
        });
        //TODO    4,提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsoObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));
        //TODO   5,按照uid分组
        KeyedStream<JSONObject, String> keyedStream = jsoObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("uid"));
        //TODO   6,使用状态编程去进行去重  并且转化为JAVAbEAN
        SingleOutputStreamOperator<UserLoginBean> userLoginDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, UserLoginBean>() {
            private ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<UserLoginBean> collector) throws Exception {
                //获取状态日期以及当前数据的日期
                String lastDt = lastLoginDtState.value();
                Long curTs = jsonObject.getLong("ts");
                String curDt = DateFormatUtil.toDate(curTs);

                //回流用户数
                long baclCt = 0L;
                //独立用户数
                long uuCt = 0L;

                if (lastDt == null) {
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                } else if (!lastDt.equals(curDt)) {
                    uuCt = 1L;
                    lastLoginDtState.update(curDt);
                    if ((curTs - DateFormatUtil.toTs(lastDt, false) / (24 * 60 * 60 * 1000L)) > 7) {
                        baclCt = 1L;
                    }
                }


                //输出数据
                if (uuCt == 1L) {
                    collector.collect(new UserLoginBean("", "", baclCt, uuCt, null));
                }
            }
        });
        //TODO    7,开窗,聚合
        SingleOutputStreamOperator<UserLoginBean> reduceDS = userLoginDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))
        ).reduce(new ReduceFunction<UserLoginBean>() {
                     @Override
                     public UserLoginBean reduce(UserLoginBean userLoginBean, UserLoginBean t1) throws Exception {
                         userLoginBean.setBackCt(userLoginBean.getBackCt() + t1.getBackCt());
                         userLoginBean.setUuCt(userLoginBean.getUuCt() + t1.getUuCt());
                         return userLoginBean;
                     }
                 }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                     @Override
                     public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                         //获取数据
                         UserLoginBean next = iterable.iterator().next();
                         //补充信息
                         next.setTs(System.currentTimeMillis());
                         next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                         next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));


                         //输出数据
                         collector.collect(next);
                     }
                 }
        );
        //todo   8,将数据写道ClickHouse
        reduceDS.print(">>>>>>>>>>>>>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_login_window values(?,?,?,?,?)"));


        //TODO 9,启动任务
        env.execute("DwsUserUserLoginWindow");
    }


}

