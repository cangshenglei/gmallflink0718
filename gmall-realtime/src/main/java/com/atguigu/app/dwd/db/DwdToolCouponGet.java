package com.atguigu.app.dwd.db;


import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

//工具域优惠劵领取事务事实表
//mock >>>>kafkaconsumer(中建表)
public class DwdToolCouponGet {
    public static void main(String[] args) {
        //TODO 1,环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //TODO 2,设置状态后端
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
        //TODO 3,从kafka中读取数据,创建为Flink SQL表
        tableEnv.executeSql("create table `topic_db`( " +
                "`database` string, " +
                "`table` string, " +
                "`data` map<string, string>, " +
                "`type` string, " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_tool_coupon_get"));

        //TODO 4,读取优惠劵领取数据,封装为表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "data['id'],  " +
                "data['coupon_id'],  " +
                "data['user_id'],  " +
                "date_format(data['get_time'],'yyyy-MM-dd') date_id,  " +
                "data['get_time'],  " +
                "ts  " +
                "from topic_db  " +
                "where `table` = 'coupon_use'  " +
                "and `type` = 'insert'  ");
        tableEnv.createTemporaryView("result_table", resultTable);
        //TODO 5,建立KAFAK-connector-dwd-tool-coupon-get表r
        tableEnv.executeSql("create table dwd_tool_coupon_get (   " +
                "id string,   " +
                "coupon_id string,   " +
                "user_id string,   " +
                "date_id string,   " +
                "get_time string,   " +
                "ts string   " +
                ")" + MyKafkaUtil.getInsertKafkaDDL("dwd_tool_coupon_get"));

        //TODO 6,将数据写入kakfa-connector表
        tableEnv.executeSql("insert into dwd_tool_coupon_get select * from result_table");
    }
}
