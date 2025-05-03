package com.jl.App;


import com.alibaba.fastjson.JSON;
import com.jl.util.KafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Properties;



public class FlinkCDC {
    //MySQL-->|CDC 捕获|Flink-->|JSON 格式过滤|Kafka
    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. 配置 Checkpoint 确保 Exactly-Once 语义
//        env.enableCheckpointing(30000);  // 30秒一次checkpoint
        Properties properties = new Properties();
        properties.setProperty("decimal.handling.mode","double");
        properties.setProperty("time.precision.mode","connect");
        // 3. 配置 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")          // MySQL 服务器地址
                .port(3306)                    // MySQL 端口
                .databaseList("realtime_v1") // 要监控的数据库
                .tableList("realtime_v1.*")  // 监控该数据库下所有表
                .username("root")     // MySQL 用户名
                .password("root")     // MySQL 密码
                .debeziumProperties(properties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将CDC数据转为JSON字符串
                .startupOptions(StartupOptions.earliest())  // 从初始快照开始，全量拿取
                .build();




        // 4. 从MySQL Source创建数据流
        DataStream<String> mysqlCDCStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source");

        // 5. 打印到控制台用于调试
        //mysqlCDCStream.print();

        SingleOutputStreamOperator<String> dbObj = mysqlCDCStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                 //
                boolean b = JSON.isValid(s);
                if (!b) {
                    return false;
                }
                if (JSON.parseObject(s).getString("after") == null) {
                    return false;
                }
                return true;
            }
        }).uid("filter_1")
                .name("filter_FlinkCDC");

        dbObj.print("------------------------------");
// 6. 配置Kafka Sink

        dbObj.addSink(KafkaUtil.getKafkaSink("topic_db"));



        // 8. 执行作业
        env.execute("FlinkCDC");
    }
}
