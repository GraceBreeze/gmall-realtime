package com.atguigu.gmall.util;

import com.atguigu.gmall.bean.TransientSink;
import com.atguigu.gmall.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yhm
 * @create 2022-08-24 8:53
 */
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getClickHouseSinkFunc(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        // 反射
                        Class<?> tClass = t.getClass();
                        //stt,edt,ts,source,keyword,keywordCount
                        Field[] fields = tClass.getDeclaredFields();
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            // 正常调用属性
                            // String s = t.属性
                            fields[i].setAccessible(true);
                            // 调用注解
                            TransientSink annotation = fields[i].getAnnotation(TransientSink.class);
                            if (annotation!=null){
                                offset ++ ;
                                continue;
                            }
                            Object o = fields[i].get(t);
                            preparedStatement.setObject(i + 1 - offset,o);
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(100L)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .build());
    }
}
