package com.zuoye.flink.iceberg;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author ZhangXueJun
 * @title IcebergWriteHiveTable
 * @date 2020/12/8 15:29
 * @projectName flink-learn
 * @description
 */
@Slf4j
public class IcebergWriteHiveTable {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hive");
        System.setProperty("HADOOP_CLASSPATH", "D:/program/hadoop-3.1.1/bin/hadoop classpath");
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inBatchMode().useBlinkPlanner().build();
        TableEnvironment tenv = TableEnvironment.create(environmentSettings);
        tenv.executeSql("CREATE CATALOG iceberg WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive'," +
                "  'uri'='thrift://hd-node-3-41.wakedata.com:9083,thrift://hd-node-3-42.wakedata.com:9083,thrift://wake-sz-vm-3-94.wakedata.com:9083', " +
                "  'clients'='5', " +
                "  'property-version'='1', " +
                "  'warehouse'='hdfs://HDFSCluster/warehouse'" +
                ")");

        tenv.useCatalog("iceberg");
        /*tenv.executeSql("CREATE DATABASE iceberg_db_whx");
        tenv.useDatabase("iceberg_db_whx");*/
        tenv.useDatabase("iceberg_db");
        tenv.executeSql(" DROP TABLE IF EXISTS iceberg.iceberg_db.zhangxuejun_iceberg_table");
        tenv.executeSql("CREATE TABLE iceberg.iceberg_db.zhangxuejun_iceberg_table (\n" +
            " userid int,\n" +
            " f_random_str STRING\n" +
            ")");
        //tenv.executeSql(
        //    "insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");

        tenv.executeSql("show tables").print();
        //   tenv.executeSql("show databases").print();
        tenv.executeSql("insert into iceberg.iceberg_db.zhangxuejun_iceberg_table values(1,'whx')");
        tenv.executeSql("insert into iceberg.iceberg_db.zhangxuejun_iceberg_table values(2,'wc')");


        tenv.executeSql("select * from iceberg.iceberg_db.zhangxuejun_iceberg_table").print();

    }
}
