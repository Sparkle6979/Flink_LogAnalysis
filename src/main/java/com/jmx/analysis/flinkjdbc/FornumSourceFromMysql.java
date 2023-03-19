package com.jmx.analysis.flinkjdbc;

import com.jmx.bean.Fornum;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * @author sparkle6979l
 * @version 1.0
 * @data 2023/3/17 22:07
 */
public class FornumSourceFromMysql extends RichSourceFunction<Fornum> {
    private PreparedStatement ps=null;
    private Connection connection=null;
    String driver = "com.mysql.jdbc.Driver";
    String tablename;
    Properties sqlproperty;


    public FornumSourceFromMysql(String tablename,Properties sqlproperty){
        this.tablename = tablename;
        this.sqlproperty = sqlproperty;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sqlsent = "select fid,name from " + this.tablename;

        ps = connection.prepareStatement(sqlsent);
    }

    @Override
    public void run(SourceContext<Fornum> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()){
            Fornum fornum = new Fornum(
                    resultSet.getInt("fid"),
                    resultSet.getString("name")
            );

            sourceContext.collect(fornum);
        }
    }

    @Override
    public void cancel() {

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
        super.close();
        if(connection != null){
            connection.close();
        }
        if (ps != null){
            ps.close();
        }
    }

}
