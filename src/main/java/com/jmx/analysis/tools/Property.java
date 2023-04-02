package com.jmx.analysis.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * @author sparkle6979l
 * @version 1.0
 * @data 2023/3/19 22:23
 */
public class Property {

    private final static String CONF_NAME = "config.properties";
    private static Properties contextProperties;

    static {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME);
        contextProperties = new Properties();

        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in,"UTF-8");
            contextProperties.load(inputStreamReader);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getValue(String key){
        return contextProperties.getProperty(key);
    }

    // 一个静态方法，得到kafka的配置文件
    public static Properties getKafkaProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", getValue("kafka.bootstrap.servers"));
        properties.setProperty("key.deserializer", getValue("kafka.key.deserializer"));
        properties.setProperty("value.deserializer", getValue("kafka.value.deserializer"));
        properties.setProperty("auto.offset.reset", getValue("kafka.auto.offset.reset"));
        properties.setProperty("group.id", groupId);
        return properties;
    }


    // 一个静态方法，得到MySQL的配置文件
    public static Properties getMySQLProperties() {
        Properties properties = new Properties();
        properties.setProperty("url", getValue("mysql.url"));
        properties.setProperty("username", getValue("mysql.username"));
        properties.setProperty("password", getValue("mysql.password"));
        return properties;
    }

}
