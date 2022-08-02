package com.atguigu.app;

import com.atguigu.bean.Bean1;
import com.atguigu.bean.Bean2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import static org.apache.avro.SchemaBuilder.map;

public class Flink01_DataStreamJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Bean1> bean1DS = env.socketTextStream("hadoop102", 8888)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean1(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean1>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean1>() {
                    @Override
                    public long extractTimestamp(Bean1 bean1, long l) {
                        return bean1.getTs() * 1000L;
                    }
                }));
        SingleOutputStreamOperator<Bean2> bean2DS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new Bean2(split[0], split[1], Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean2>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Bean2>() {
                    @Override
                    public long extractTimestamp(Bean2 bean2, long l) {
                        return bean2.getTs() * 1000L;
                    }
                }));
        SingleOutputStreamOperator<Tuple2<Bean1, Bean2>> jionDS = bean1DS.keyBy(Bean1::getId)
                .intervalJoin(bean2DS.keyBy(Bean2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Bean1, Bean2, Tuple2<Bean1, Bean2>>() {
                    @Override
                    public void processElement(Bean1 bean1, Bean2 bean2, Context context, Collector<Tuple2<Bean1, Bean2>> collector) throws Exception {
                        collector.collect(new Tuple2<>(bean1, bean2));
                    }
                });

        jionDS.print(">>>>>>>>>>>>>>>>>");
        env.execute();


    }
}
