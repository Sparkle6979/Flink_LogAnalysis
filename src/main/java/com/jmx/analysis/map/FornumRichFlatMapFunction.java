package com.jmx.analysis.map;

import com.jmx.analysis.tools.AnalysisTools;
import com.jmx.bean.Fornum;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author sparkle6979l
 * @version 1.0
 * @data 2023/3/18 16:56
 */
public class FornumRichFlatMapFunction extends RichFlatMapFunction<Tuple4<String, String, Integer, Integer>, Tuple3<Integer,String,Long>> {
    private PreparedStatement ps=null;
    private Connection connection=null;
    private String driver = "com.mysql.jdbc.Driver";
    private String tablename;
    private Properties sqlproperty;

    HashMap<Integer, Fornum> fornumInfo = new HashMap<>();

    public FornumRichFlatMapFunction(String tablename,Properties sqlproperty){
        this.tablename = tablename;
        this.sqlproperty = sqlproperty;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();
        String sqlsent = "select fid,name from " + this.tablename;

        ps = connection.prepareStatement(sqlsent);

        // 静态加载

        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            Fornum fornum = new Fornum(
                    resultSet.getInt("fid"),
                    resultSet.getString("name"));

            fornumInfo.put(fornum.fid,fornum);
        }

//        System.out.println(fornumInfo);
    }

    @Override
    public void flatMap(Tuple4<String, String, Integer, Integer> accessLogRecord, Collector<Tuple3<Integer,String,Long>> collector) throws Exception {
        if (!fornumInfo.containsKey(accessLogRecord.f2))
            return ;
        Fornum fornum = fornumInfo.get(accessLogRecord.f2);
        Long datetime = AnalysisTools.Timestamp2long(accessLogRecord.f1);
        collector.collect(new Tuple3<Integer,String,Long>(fornum.fid,fornum.name,datetime));
    }

    // 配置mysql
    public Connection getConnection(){
        try {
            //加载驱动
            Class.forName(driver);
            //创建连接
            connection = DriverManager.getConnection(sqlproperty.getProperty("url"),sqlproperty.getProperty("username"),sqlproperty.getProperty("password"));
        } catch (Exception e) {
            System.out.println("********mysql get connection occur exception, msg = "+e.getMessage());
            e.printStackTrace();
        }
        return  connection;
    }

    @Override
    public void close() throws Exception {
        connection.close();
        ps.close();
    }



}
