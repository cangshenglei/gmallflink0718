package com.atguigu.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.UserRegisterBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

//数据流：web/app -> Nginx -> 业务服务器 -> Mysql(binlog) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock -> Mysql(binlog) -> Maxwell -> Kafka(ZK) -> DwdUserRegister -> Kafka(ZK) -> DwsUserUserRegisterWindow -> ClickHouse(ZK)
public class DwsUserUserRegisterWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1,获取执 行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        env.setParallelism(1);
        //
        //        // 1.1 状态后端设置
        ////        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        ////        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        ////        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        ////        env.getCheckpointConfig().enableExternalizedCheckpoints(
        ////                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        ////        );
        ////        env.setRestartStrategy(RestartStrategies.failureRateRestart(
        ////                3, Time.days(1), Time.minutes(1)
        ////        ));
        ////        env.setStateBackend(new HashMapStateBackend());
        ////        env.getCheckpointConfig().setCheckpointStorage(
        ////                "hdfs://hadoop102:8020/ck"
        ////        );
        ////        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //TODO 2,读取DWD层用户注册主题数据创建流并转为数据结构JavaBean
        String topic = "dwd_user_register";
        String groupId = "user_register_220212";
        SingleOutputStreamOperator<UserRegisterBean> userRegisterDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId))
                .map(line -> {
                    JSONObject jsonObject = JSON.parseObject(line);
                    return new UserRegisterBean("", "", 1L,
                            DateFormatUtil.toTs(jsonObject.getString("create_time"), true)
                    );
                });

        //TODO 3,提取时间戳生成WaterMark
        SingleOutputStreamOperator<UserRegisterBean> userRegisterWithWmDS = userRegisterDS.assignTimestampsAndWatermarks(WatermarkStrategy.<UserRegisterBean>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserRegisterBean>() {
            @Override
            public long extractTimestamp(UserRegisterBean userRegisterBean, long l) {
                return userRegisterBean.getTs();
            }
        }));

        //TODO 4,开窗,聚合
        SingleOutputStreamOperator<UserRegisterBean> reduceDS = userRegisterWithWmDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserRegisterBean>() {
                            @Override
                            public UserRegisterBean reduce(UserRegisterBean userRegisterBean, UserRegisterBean t1) throws Exception {
                                userRegisterBean.setRegisterCt(userRegisterBean.getRegisterCt() + t1.getRegisterCt());
                                return userRegisterBean;
                            }
                        }, new AllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow timeWindow, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                                UserRegisterBean next = iterable.iterator().next();
                                next.setTs(System.currentTimeMillis());
                                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));
                                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                                collector.collect(next);
                            }
                        }

                );
        //TODO 5,写出数据到ClickHouse
        reduceDS.print(">>>>>>>>>>>>>>>>>>>>>");
        reduceDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_user_user_register_window values(?,?,?,?)"));
        //TODO 6,启动
        env.execute("DwsUserUserRegisterWindow");


    }


}
