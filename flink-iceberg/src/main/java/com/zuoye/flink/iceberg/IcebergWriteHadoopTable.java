package com.zuoye.flink.iceberg;

/**
 * @author ZhangXueJun
 * @title IcebergWriteHadoopTable
 * @date 2020/12/8 15:37
 * @projectName flink-learn
 * @description
 */
public class IcebergWriteHadoopTable extends IcebergBatchWrite {

    public static void main(String[] args) {
        tableEnv.executeSql(" CREATE CATALOG iceberg_catalog WITH (\n" +
                "   'type'='iceberg',\n" +
                "   'catalog-type'='hadoop',\n" +
                "   'clients'='5',\n" +
                "   'property-version'='1',\n" +
                "   'warehouse'='hdfs://HDFSCluster/warehouse/iceberg_catalog'\n" +
                " );");

        tableEnv.useCatalog("iceberg");
        /*tenv.executeSql("CREATE DATABASE iceberg_db_whx");
        tenv.useDatabase("iceberg_db_whx");*/
        tableEnv.useDatabase("iceberg_db");
        tableEnv.executeSql(" DROP TABLE IF EXISTS iceberg.iceberg_db.zhangxuejun_iceberg_table");
        tableEnv.executeSql("CREATE TABLE iceberg.iceberg_db.zhangxuejun_iceberg_table (\n" +
                " userid int,\n" +
                " f_random_str STRING\n" +
                ")");
        //tenv.executeSql(
        //    "insert into iceberg.iceberg_db.iceberg_001 select * from iceberg.iceberg_db.sourceTable");

        tableEnv.executeSql("show tables").print();
        //   tenv.executeSql("show databases").print();
        tableEnv.executeSql("insert into iceberg.iceberg_db.zhangxuejun_iceberg_table values(1,'whx')");
        tableEnv.executeSql("insert into iceberg.iceberg_db.zhangxuejun_iceberg_table values(2,'wc')");


        tableEnv.executeSql("select * from iceberg.iceberg_db.zhangxuejun_iceberg_table").print();
    }

}
