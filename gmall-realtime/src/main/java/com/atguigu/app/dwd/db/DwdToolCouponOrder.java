package com.atguigu.app.dwd.db;


import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//优惠券使用下单事务实时表
public class DwdToolCouponOrder {
    public static void main(String[] args) {

        //TODO 1,创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2,后端配置
        /*   env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/
        //TODO 3,从kafka中读取数据  DATA
        tableEnv.executeSql("create table `topic_db` (   " +
                "`database` string,   " +
                "`table` string,   " +
                "`data` map<string, string>,   " +
                "`type` string,   " +
                "`old` map<string, string>,   " +
                "`ts` string   " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_order"));
        //todo 4,使用FLink SQL 创建表  选取字段
        Table couponUseOrder = tableEnv.sqlQuery("select   " +
                "data['id'] id,   " +
                "data['coupon_id'] coupon_id,   " +
                "data['user_id'] user_id,   " +
                "data['order_id'] order_id,   " +
                "date_format(data['using_time'],'yyyy-MM-dd') date_id,   " +
                "data['using_time'] using_time,   " +
                "ts   " +
                "from topic_db   " +
                "where `table` = 'coupon_use'   " +
                "and `type` = 'update'   " +
                "and data['coupon_status'] = '1402'   " +
                "and `old`['coupon_status'] = '1401'");
        tableEnv.createTemporaryView("result_table", couponUseOrder);
        //TODO 5,读取符合需求的诗句,封装创建临时表,
        tableEnv.executeSql("create table dwd_tool_coupon_order(   " +
                "id string,   " +
                "coupon_id string,   " +
                "user_id string,   " +
                "order_id string,   " +
                "date_id string,   " +
                "order_time string,   " +
                "ts string   " +
                ")" + MyKafkaUtil.getInsertKafkaDDL("dwd_tool_coupon_order"));

        //TODO 6,创建KAFKA 相关的表
        //TODO 7,将数据写入
        tableEnv.executeSql("" +
                "insert into dwd_tool_coupon_order select " +
                "id,   " +
                "coupon_id,   " +
                "user_id,   " +
                "order_id,   " +
                "date_id,   " +
                "using_time order_time,   " +
                "ts from result_table");
    }
}
