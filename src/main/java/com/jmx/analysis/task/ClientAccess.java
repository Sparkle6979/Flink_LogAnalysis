package com.jmx.analysis.task;

import com.jmx.analysis.tools.AnalysisTools;
import com.jmx.analysis.LogAnalysis;
import com.jmx.bean.AccessLogRecord;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author sparkle6979l
 * @version 1.0
 * @data 2023/3/19 15:51
 */
public class ClientAccess {
    public static void main(String[] args) throws Exception {
        // 创建虚拟环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        // kafka参数配置
        Properties props = new Properties();
        // kafka broker地址
        props.put("bootstrap.servers", "172.16.240.10:9092");
        // 消费者组
        props.put("group.id", "log_consumer");
        // kafka 消息的key序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka 消息的value序列化器
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");


        Properties sqlprops = new Properties();
        sqlprops.put("url", "jdbc:mysql://localhost:3306/ultrax");
        sqlprops.put("username", "root");
        sqlprops.put("password", "123456");

        // 获取 log 的 DataSource
        DataStream<String> logSource = env.addSource(new FlinkKafkaConsumer<String>("user_access_logs", new SimpleStringSchema(), props));

//        DataStream<String> logSource = env.readTextFile("/Users/sparkle6979l/Mavens/FlinkStu/flink-tes/lampp/logs/access_log");

        // log message的筛选与处理
        DataStream<AccessLogRecord> AvaliableLog = AnalysisTools.getAvailableAccessLog(logSource);
        // 获取[clienIP,accessDate,sectionId,articleId]
        SingleOutputStreamOperator<Tuple4<String, Integer, Long, Long>> process = LogAnalysis.getFieldFromLog(AvaliableLog)
                // 水位线生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, Integer, Integer>>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<Tuple4<String, String, Integer, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, Integer, Integer> stringStringIntegerIntegerTuple4, long l) {
                                return AnalysisTools.Timestamp2long(stringStringIntegerIntegerTuple4.f1);
                            }
                        }
                ))
                // 按ClientIP分组
                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new AccessCnt());


        process.addSink(JdbcSink.sink(
                "INSERT INTO client_ip_access(client,client_access_cnt,time_beg,time_end) VALUES(?,?,?,?)",
                ((preparedStatement, integerStringIntegerTuple4) -> {
                    preparedStatement.setString(1, integerStringIntegerTuple4.f0);
                    preparedStatement.setInt(2, integerStringIntegerTuple4.f1);
                    preparedStatement.setString(3, AnalysisTools.Long2timestamp(integerStringIntegerTuple4.f2));
                    preparedStatement.setString(4, AnalysisTools.Long2timestamp(integerStringIntegerTuple4.f3));

                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(sqlprops.getProperty("url"))
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername(sqlprops.getProperty("username"))
                        .withPassword(sqlprops.getProperty("password"))
                        .build()


        ));

        env.execute();
    }



    public static class AccessCnt extends ProcessWindowFunction<Tuple4<String, String,Integer, Integer>, Tuple4<String, Integer, Long, Long>, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Tuple4<String, String, Integer, Integer>, Tuple4<String, Integer, Long, Long>, String, TimeWindow>.Context context, Iterable<Tuple4<String, String, Integer, Integer>> iterable, Collector<Tuple4<String, Integer, Long, Long>> collector) throws Exception {
            int cnt = 1;
            String name = "";
            for (Tuple4<String, String,Integer, Integer> tmp : iterable) {
                name = tmp.f0;
                ++cnt;
            }
            collector.collect(new Tuple4<String, Integer, Long, Long>(name, cnt, context.window().getStart(), context.window().getEnd()));
        }
    }

}
