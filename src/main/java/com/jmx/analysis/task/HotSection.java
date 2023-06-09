package com.jmx.analysis.task;

import com.jmx.analysis.tools.AnalysisTools;
import com.jmx.analysis.map.FornumRichFlatMapFunction;
import com.jmx.analysis.tools.Property;
import com.jmx.bean.AccessLogRecord;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.*;
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
 * @data 2023/3/17 16:23
 */
public class HotSection {

    public static void main(String[] args) throws Exception {

        // 创建虚拟环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka 配置文件加载
        Properties props = Property.getKafkaProperties("log_consumer");
        // Mysql 配置文件加载
        Properties sqlprops = Property.getMySQLProperties();


        DataStream<String> logSource = env.addSource(new FlinkKafkaConsumer<String>("user_access_logs", new SimpleStringSchema(), props));

//        DataStream<String> logSource = env.readTextFile("/Users/sparkle6979l/Mavens/FlinkStu/flink-tes/lampp/logs/access_log");


        DataStream<AccessLogRecord> AvaliableLog = AnalysisTools.getAvailableAccessLog(logSource);
        // 获取[clienIP,accessDate,sectionId,articleId]
        DataStream<Tuple4<String, String, Integer, Integer>> fieldFromLog = AnalysisTools.getFieldFromLog(AvaliableLog);

        SingleOutputStreamOperator<Tuple5<Integer, String, Integer, Long, Long>> pre_forum_forum = fieldFromLog
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
                        .keyBy(data -> data.f4)
                                .process(new HotArticle.TopNProcessFunction(1));




        pre_forum_forum.addSink(JdbcSink.sink(
                "INSERT INTO hot_section(section_id,name,section_pv,time_beg,time_end) VALUES(?,?,?,?,?)",
                ((preparedStatement, integerStringIntegerTuple3) -> {
                    preparedStatement.setInt(1,integerStringIntegerTuple3.f0);
                    preparedStatement.setString(2,integerStringIntegerTuple3.f1);
                    preparedStatement.setInt(3,integerStringIntegerTuple3.f2);
                    preparedStatement.setString(4,AnalysisTools.Long2timestamp(integerStringIntegerTuple3.f3));
                    preparedStatement.setString(5,AnalysisTools.Long2timestamp(integerStringIntegerTuple3.f4));

                }),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(sqlprops.getProperty("url"))
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername(sqlprops.getProperty("username"))
                        .withPassword(sqlprops.getProperty("password"))
                        .build()
        ));
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






