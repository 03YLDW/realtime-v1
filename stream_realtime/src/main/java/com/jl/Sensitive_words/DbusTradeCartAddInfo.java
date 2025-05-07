package com.jl.Sensitive_words;

import com.jl.utils.ConfigUtils;
import com.jl.utils.EnvironmentSettingUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DbusTradeCartAddInfo {
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_cdc_db_topic = ConfigUtils.getString("kafka.cdc.db.topic");
    private static final String catalog_minio_name = "minio_paimon_catalog";
    private static final String minio_database_name = "realtime_v1";



    public static void main(String[] args) {
        //强制设定Hadoop访问身份，解决权限问题
        System.setProperty("HADOOP_USER_NAME","root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

    }
}
