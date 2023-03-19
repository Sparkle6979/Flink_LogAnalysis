package com.jmx.analysis.map;

import com.jmx.analysis.tools.AnalysisTools;
import com.jmx.bean.Article;
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
 * @data 2023/3/19 15:02
 */
public class AticleRichFlatMapFunction extends RichFlatMapFunction<Tuple4<String, String, Integer, Integer>, Tuple3<Integer,String,Long>> {
    private PreparedStatement ps=null;
    private Connection connection=null;
    private String driver = "com.mysql.jdbc.Driver";
    private String tablename;
    private Properties sqlproperty;

    HashMap<Integer, Article> articleInfo = new HashMap<>();

    public AticleRichFlatMapFunction(String tablename,Properties sqlproperty){
        this.tablename = tablename;
        this.sqlproperty = sqlproperty;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = getConnection();
        String sqlsent = "select tid,subject from " + this.tablename;

        ps = connection.prepareStatement(sqlsent);

        // 静态加载

        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            if(resultSet.getString("subject").equals(""))
                continue;
            Article article = new Article(
                    resultSet.getInt("tid"),
                    resultSet.getString("subject"));

            articleInfo.put(article.aid,article);
        }

    }

    @Override
    public void flatMap(Tuple4<String, String, Integer, Integer> accessLogRecord, Collector<Tuple3<Integer,String,Long>> collector) throws Exception {
        if (!articleInfo.containsKey(accessLogRecord.f3))
            return ;
        Article article = articleInfo.get(accessLogRecord.f3);
        Long datetime = AnalysisTools.Timestamp2long(accessLogRecord.f1);
        collector.collect(new Tuple3<Integer,String,Long>(article.aid,article.name,datetime));
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
