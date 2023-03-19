package com.jmx.analysis;

import com.jmx.analysis.flinkjdbc.FornumSourceFromMysql;
import com.jmx.analysis.map.FornumRichFlatMapFunction;
import com.jmx.bean.AccessLogRecord;
import com.jmx.bean.Fornum;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;

import org.apache.flink.connector.jdbc.JdbcInputFormat;

/**
 * @author sparkle6979l
 * @version 1.0
 * @data 2023/3/17 16:23
 */
public class DataStreamLogAnalysis {

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


//        DataStream<String> logSource = env.addSource(new FlinkKafkaConsumer<String>("user_access_logs", new SimpleStringSchema(), props));


        DataStream<String> logSource = env.readTextFile("/Users/sparkle6979l/Mavens/FlinkStu/flink-tes/lampp/logs/access_log");

//
//        DataStream<Fornum> fornumSource = env.addSource(new FornumSourceFromMysql("pre_forum_forum",sqlprops));
//
        DataStream<AccessLogRecord> AvaliableLog = AnalysisTools.getAvailableAccessLog(logSource);


        // 获取[clienIP,accessDate,sectionId,articleId]
        DataStream<Tuple4<String, String, Integer, Integer>> fieldFromLog = LogAnalysis.getFieldFromLog(AvaliableLog);


//        SingleOutputStreamOperator<Tuple3<Integer, String, Integer>> pre_forum_forum =
        fieldFromLog
                .flatMap(new FornumRichFlatMapFunction("pre_forum_forum", sqlprops))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Integer, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Integer, String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple3<Integer, String, Long> integerStringStringTuple3, long l) {
                                return integerStringStringTuple3.f2;
                            }
                        }))

                .keyBy(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.days(3)))
                .process(new CntSectionID())
                .print();

//                .map(new MapFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>>() {
//
//                    @Override
//                    public Tuple3<Integer, String, Integer> map(Tuple2<Integer, String> integerStringTuple2) throws Exception {
//                        return new Tuple3<Integer, String, Integer>(integerStringTuple2.f0, integerStringTuple2.f1, 1);
//                    }
//                })
//                .keyBy(data -> data.f0)
//                .sum(2);
//
//        pre_forum_forum.addSink(JdbcSink.sink(
//                "INSERT INTO hot_section(section_id,name,section_pv,statistic_time) VALUES(?,?,?,?)",
//                ((preparedStatement, integerStringIntegerTuple3) -> {
//                    preparedStatement.setInt(1,integerStringIntegerTuple3.f0);
//                    preparedStatement.setString(2,integerStringIntegerTuple3.f1);
//                    preparedStatement.setInt(3,integerStringIntegerTuple3.f2);
//                    preparedStatement.setTimestamp(4,new Timestamp(System.currentTimeMillis()));
//                }),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl(sqlprops.getProperty("url"))
//                        .withDriverName("com.mysql.jdbc.Driver")
//                        .withUsername(sqlprops.getProperty("username"))
//                        .withPassword(sqlprops.getProperty("password"))
//                        .build()
//        ));
//
//
        env.execute();

    }

    public static class CntSectionID extends ProcessWindowFunction<Tuple3<Integer, String, Long>, Tuple5<Integer, String, Integer, Long, Long>, Integer, TimeWindow> {
        @Override
        public void process(Integer integer, ProcessWindowFunction<Tuple3<Integer, String, Long>, Tuple5<Integer, String, Integer, Long, Long>, Integer, TimeWindow>.Context context, Iterable<Tuple3<Integer, String, Long>> iterable, Collector<Tuple5<Integer, String, Integer, Long, Long>> collector) throws Exception {
            int cnt = 1;
            String name = "";
            for (Tuple3<Integer, String, Long> tmp : iterable) {
                name = tmp.f1;
                ++cnt;
            }
            collector.collect(new Tuple5<>(integer, name, cnt, context.window().getStart(), context.window().getEnd()));
        }
    }


}






