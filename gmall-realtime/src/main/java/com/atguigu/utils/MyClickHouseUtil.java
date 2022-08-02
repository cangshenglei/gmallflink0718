package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {
    //<T>  <T>定义泛型的方法
    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // //preparedStatement => insert into t values(?,?,?,?,?,?)
                        //通过java的高级编程 反射 来获取所有的字段
                        Class<?> clz = t.getClass();
                        //Field[] fields = clz.getFields();
                        Field[] fields = clz.getDeclaredFields();
                        //todo bean表中的原注释被跳过,在跳过之后,依据角注无法一一对应,出现了原标注之后多了一个索引值
                        int j = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            field.setAccessible(true);

                            //获取字段的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                j++;
                                continue;
                            }
                            //获取值
                            Object value = field.get(t);
                            preparedStatement.setObject(i + 1 - j, value);
                        }
                    }
                }, new JdbcExecutionOptions.Builder().withBatchSize(5)
                        .withBatchIntervalMs(1000L)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL).build()
        );


    }

}
