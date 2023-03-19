package com.jmx.analysis;

import com.jmx.bean.AccessLogRecord;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author sparkle6979l
 * @version 1.0
 * @data 2023/3/17 20:52
 */
public class AnalysisTools {

    public static DataStream<AccessLogRecord> getAvailableAccessLog(DataStream<String> accessLog){
        SingleOutputStreamOperator<AccessLogRecord> result = accessLog
                .map(new MapFunction<String, AccessLogRecord>() {
                    @Override
                    public AccessLogRecord map(String s) throws Exception {
                        return new LogParse().parseRecord(s);
                    }
                })
                // 筛去值为null的Log
                .filter(new FilterFunction<AccessLogRecord>() {
                    @Override
                    public boolean filter(AccessLogRecord accessLogRecord) throws Exception {
                        return !(accessLogRecord == null);
                    }
                })
                // 筛去请求失败的Log
                .filter(new FilterFunction<AccessLogRecord>() {
                    @Override
                    public boolean filter(AccessLogRecord accessLogRecord) throws Exception {
                        return accessLogRecord.getHttpStatusCode().equals("200");
                    }
                });
        return result;
    }


    public static DataStream<Tuple4<String, String, Integer, Integer>> getFieldFromLog(DataStream<AccessLogRecord> log_access){
        DataStream<Tuple4<String, String, Integer, Integer>> result = log_access
                .map(new MapFunction<AccessLogRecord, Tuple4<String, String, Integer, Integer>>() {
                    @Override
                    public Tuple4<String, String, Integer, Integer> map(AccessLogRecord accessLogRecord) throws Exception {
                        LogParse parse = new LogParse();

                        String clientIpAddress = accessLogRecord.getClientIpAddress();
                        String dateTime = accessLogRecord.getDateTime();
                        String request = accessLogRecord.getRequest();
                        String formatDate = parse.parseDateField(dateTime);

                        Tuple2<String, String> sectionIdAndArticleId = parse.parseSectionIdAndArticleId(request);

                        if (formatDate == "" || sectionIdAndArticleId == Tuple2.of("", "")) {

                            return new Tuple4<String, String, Integer, Integer>("0.0.0.0", "0000-00-00 00:00:00", 0, 0);
                        }
                        Integer sectionId = (sectionIdAndArticleId.f0 == "") ? 0 : Integer.parseInt(sectionIdAndArticleId.f0);
                        Integer articleId = (sectionIdAndArticleId.f1 == "") ? 0 : Integer.parseInt(sectionIdAndArticleId.f1);
                        return new Tuple4<>(clientIpAddress, formatDate, sectionId, articleId);
                    }
        });
        return result;
    }



    public static Long Timestamp2long(String time){
        Calendar calendar = Calendar.getInstance();
        try {
            calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return calendar.getTimeInMillis();
    }
}
