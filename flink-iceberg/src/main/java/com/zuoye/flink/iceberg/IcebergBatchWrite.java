package com.zuoye.flink.iceberg;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author ZhangXueJun
 * @title IcebergBatchWriteTest
 * @date 2020/12/9 11:38
 * @projectName flink-learn
 * @description
 */
public class IcebergBatchWrite {

    protected static TableEnvironment tableEnv;

    static {
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("HADOOP_CLASSPATH", "D:/program/hadoop-3.1.1/bin/hadoop classpath");
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        tableEnv = TableEnvironment.create(environmentSettings);

        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("akka.ask.timeout", "10 min");
    }
}