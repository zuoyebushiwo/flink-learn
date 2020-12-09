package com.zuoye.flink.iceberg;

import lombok.extern.slf4j.Slf4j;

/**
 * @author ZhangXueJun
 * @title IcebergWriteHiveTable
 * @date 2020/12/8 15:29
 * @projectName flink-learn
 * @description
 */
@Slf4j
public class IcebergWriteHiveTable extends IcebergBatchWrite {

    public static void main(String[] args) {
        tableEnv.executeSql("CREATE CATALOG iceberg WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hive'," +
                "  'uri'='thrift://hd-node-3-41.wakedata.com:9083,thrift://hd-node-3-42.wakedata.com:9083,thrift://wake-sz-vm-3-94.wakedata.com:9083', "
                +
                "  'clients'='5', " +
                "  'property-version'='1', " +
                "  'warehouse'='hdfs://HDFSCluster/warehouse'" +
                ")");

        tableEnv.useCatalog("iceberg");
        /*tableEnv.executeSql("CREATE DATABASE iceberg_db_whx");
        tableEnv.useDatabase("iceberg_db_whx");*/
        tableEnv.useDatabase("iceberg_db");
        tableEnv.executeSql(" DROP TABLE IF EXISTS iceberg.iceberg_db.zhangxuejun_iceberg_table");
        tableEnv.executeSql("CREATE TABLE iceberg.iceberg_db.zhangxuejun_iceberg_table (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ")  WITH ('write.format.default'='orc')");

        //tableEnv.executeSql(
        //    "insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");

        tableEnv.executeSql("show tables").print();
        tableEnv.executeSql("show databases").print();
        tableEnv.executeSql("insert into iceberg.iceberg_db.zhangxuejun_iceberg_table values(10,'whx')");
        tableEnv.executeSql("insert into iceberg.iceberg_db.zhangxuejun_iceberg_table values(20,'wc')");


        tableEnv.executeSql("select * from iceberg.iceberg_db.zhangxuejun_iceberg_table").print();
    }
}
