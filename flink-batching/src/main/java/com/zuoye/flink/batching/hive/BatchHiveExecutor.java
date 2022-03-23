package com.zuoye.flink.batching.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author ZhangXueJun
 * @Date 2022年03月22日
 */
public class BatchHiveExecutor {

    private String database = "default";
    private String confDir = "/opt/hive-conf";
    private String name = "myhive";

    private EnvironmentSettings.Builder settings = EnvironmentSettings.newInstance().inBatchMode();
    private TableEnvironment tableEnv = TableEnvironment.create(settings.build());

    public static void main(String[] args) {
        BatchHiveExecutor batchHiveConnector = new BatchHiveExecutor();
        batchHiveConnector.execute();
    }

    private void execute() {
        executeBefore();
        executeInternal();
        executeAfter();
    }

    private void executeBefore() {
        HiveCatalog hive = new HiveCatalog(name, database, confDir);
        tableEnv.registerCatalog(name, hive);
        tableEnv.useCatalog(name);
    }

    private void executeInternal() {
    }

    private void executeAfter() {
    }
}
