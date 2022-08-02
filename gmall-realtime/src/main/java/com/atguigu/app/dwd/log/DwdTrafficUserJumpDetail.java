package com.atguigu.app.dwd.log;


import com.alibaba.fastjson.JSON;
import com.atguigu.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.var;
import org.apache.flink.cep.CEP;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

//数据流：web/app -> nginx -> 日志服务器(File) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：Mock_log -> Flume(f1.sh) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUserJumpDetail -> Kafka(ZK)
public class DwdTrafficUserJumpDetail {
    public static void main(String[] args) throws Exception {
        //1,TODO 获取执行环境
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

        //2,TODO 读取Kafak页面主题数据创建流
        String pageTopic = "dwd_traffic_page_log";
        String groupId = "user_jump_detail_0212";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(pageTopic, groupId));

        //3,TODO 转化为JSON对象 并且过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                try {
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("脏数据：" + s);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                return jsonObject.getLong("ts");
            }
        }));

        //4, TODO 依据MID分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        //5,TODO 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
                                                                                               @Override
                                                                                               public boolean filter(JSONObject jsonObject) throws Exception {
                                                                                                   return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                                                                                               }

                                                                                           }
        ).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //6,TODO 将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //7,TODO 提取事件(注意超时事件提取)
        OutputTag<String> outputTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<String> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
                    @Override
                    public String timeout(Map<String, List<JSONObject>> map, long timeoutTimestamp) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override
                    public String select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0).toJSONString();
                    }
                }
        );
        DataStream<String> timeOutDS = selectDS.getSideOutput(outputTag);

        // 8,TODO 合并两个流
        DataStream<String> unionDS = selectDS.union(timeOutDS);
        //9 TODO 将数据写出到Kafka
        selectDS.print("selectDS>>>>>>");
        timeOutDS.print("timeOutDS>>>>>>");
        String targetTopic = "dwd_traffic_user_jump_detail";
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));
        //TODO 10,启动任务
        env.execute("DwdTrafficUserJumpDetail");


    }
}
