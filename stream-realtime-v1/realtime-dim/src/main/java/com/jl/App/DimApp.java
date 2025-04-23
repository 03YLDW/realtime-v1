package com.jl.App;

import com.jl.util.KafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jl.bean.TableProcessDim;
import com.jl.util.HBaseUtil;
import com.jl.contant.Constant;
import com.jl.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Package com.jl.realtime_ods.ods.Flink_to_kfk
 * @Author jia.le
 * @Date 2025/4/8 16:02
 * @description: 1
 */

public class DimApp {

    @SneakyThrows
    public static void main(String[] args) {
        //todo 1.基本环节准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //todo 2. 检查点相关的设置
        //2.1 开启检查点
//        //分布式快照算法（基于Chandy-Lamport算法的分布式快照）
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        //2.3 设置job取消后检查点是否保留
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 设置两个检查之间的最小时间间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        //固定延迟重启  重启三次   3秒尝试一次
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //  三十天内有三次机会,三秒尝试一次
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端以及检查点存储路径
        //状态后端的两种  1 HashMapStateBackend  状态存在 TaskManager，状态的备份也就是检查点是存在JobManager的堆内存里
        //             2    RocksDB
//        env.setStateBackend(new HashMapStateBackend());
//        //不让检查点存储在内存中，另外设置检查点的存储位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck");
//        //2.7 设置操作hadoop的用户
//        System.setProperty("HADOOP_USER_NAME","root");
        //todo 3. 从Kafka的topic_db 主题中读取业务数据
        //3.1 声明消费的主题以及消费者组
        String topic="topic_db";

        //3.2创建消费者对象
        /*
//        KafkaSource<String> kafkasource = KafkaSource.<String>builder()
//                //设置kfk地址
//                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
//                .setTopics(topic)
//                .setGroupId(group)
//                //消费位点
//                // 从最末尾位点开始消费
//                //在生产环境中，一般为了保证消费的精准一次性，需要手动维护偏移量，KafkaSource->KafkaSourceReader->存储偏移量变
//                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
//                //
//                .setStartingOffsets(OffsetsInitializer.earliest())// 全量
//                //反序列化方式
//                //注意：如果使用Flink提供的SimpleStringSchema 对String类型的消息进行反序列化，如果kfk有消息为空，会报错
////              .setValueOnlyDeserializer(new SimpleStringSchema())
//                .setValueOnlyDeserializer(
//                        new DeserializationSchema<String>() {
//                            @Override
//                            public String deserialize(byte[] message) throws IOException {
//                                if (message!=null){
//                                    return new String(message);
//                                }
//                                return null;
//                            }
//
//                            @Override
//                            public boolean isEndOfStream(String nextElement) {
//                                return false;
//                            }
//
//                            @Override
//                            public TypeInformation<String> getProducedType() {
//                                return TypeInformation.of(String.class);
//                            }
//                        }
//                )
//                .build();

*/



        DataStreamSource<String> addd = KafkaUtil.getKafkaSource(env, topic, "addd");
//        addd.print("-------------------------------->");
//        addd.print();


        //3.3消费数据 封装为流
//        DataStreamSource<String> kafkaStreamDS = env.fromSource(kafkasource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
//            //todo 4.对读取的数进行转换 并进行简单的ETL  jsonStr->jsonObj

        SingleOutputStreamOperator<JSONObject> process = addd.map(JSONObject::parseObject);

//        SingleOutputStreamOperator<JSONObject> process = process1.filter(new FilterFunction<JSONObject>() {
//            @Override
//            public boolean filter(JSONObject value) throws Exception {
//
//                return "user_info".equals(value.getJSONObject("source").getString("table"));
//            }
//        });

        //todo 5.使用flinkcdc读取配置信息

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")          // MySQL 服务器地址
                .port(3306)                    // MySQL 端口
                .databaseList("realtime_dim") // 要监控的数据库
                .tableList("realtime_dim.table_process_dim")  // 监控该数据库下所有表
                .username("root")     // MySQL 用户名
                .password("root")     // MySQL 密码
                .deserializer(new JsonDebeziumDeserializationSchema()) // 将CDC数据转为JSON字符串
                .startupOptions(StartupOptions.initial())  // 从初始快照开始
                .build();


        DataStream<String> mysqlCDCStream = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source");
//        mysqlCDCStream.print();
//        {"before":null,"after":{"source_table":"base_region","sink_table":"dim_base_region","sink_family":"info","sink_columns":"id,region_name","sink_row_key":"id"},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":0,"snapshot":"false","db":"realtime_dim","sequence":null,"table":"table_process_dim","server_id":0,"gtid":null,"file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1744256912178,"transaction":null}

        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlCDCStream.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String op = jsonObject.getString("op");
                TableProcessDim tableProcessDim = null;
                if ("d".equals(op)) {
                    //对表进行一次删除操作   从before属性中获取删除前的配置信息
                    tableProcessDim = jsonObject.getObject("before", TableProcessDim.class);

                } else {
                    tableProcessDim = jsonObject.getObject("after", TableProcessDim.class);
                }
                tableProcessDim.setOp(op);
                return tableProcessDim;
            }
        }).setParallelism(1);
//        tbDS.print(); //展示的是创建了哪些表
        //todo 7.根据配置表中的配置信息到HBase中执行
        // 参数格式：ZooKeeper集群节点（逗号分隔）


/*
 //        tpDS.map(new RichMapFunction<TableProcessDim, TableProcessDim>() {
 //
 //            private Connection connection;
 //
 //            @Override
 //            public void open(Configuration parameters) throws Exception {
 //
 //                connection = HBaseUtil.getHBaseConnection();
 //
 //            }
 //
 //
 //            @Override
 //            public void close() throws Exception {
 //                HBaseUtil.closeHBaseConnection(connection);
 //
 //            }
 //                @Override
 //            public TableProcessDim map(TableProcessDim tp) throws Exception {
 //
 //                String op = tp.getOp();
 //                String sinkTable = tp.getSinkTable();
 //                String[] sinkFamilys = tp.getSinkFamily().split(",");
 //
 //                if ("d".equals(op)) {
 //                    HBaseUtil.dropHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable);
 //                } else if ("r".equals(op) || "c".equals(op)) {
 //                    HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilys);
 //                } else {
 //                    HBaseUtil.dropHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable);
 //                    HBaseUtil.createHBaseTable(connection, Constant.HBASE_NAMESPACE, sinkTable, sinkFamilys);
 //
 //                }
 //
 //
 //                return tp;
 //            }
 //        }).setParallelism(1);
 */

        //todo 8.将配置流中的配置信息进行广播-----broadcast
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor
                //                                                                           k:维度表表名  v:每一行的数据，也就是封装的实体类对象
                = new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        //todo 9.将主流业务数据和广播流配置信息进行关联----connect
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = process.connect(broadcastDS);



        //todo 10.处理关联后的数据(判断是否为维度)
        //processElement 处理主流业务数据    根据维度表到广播流中读取配置信息，判断是否为维度
        //processBroadcastElement:处理广播流配置信息    将配置数据放到广播状态中

        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDim>> dimDS = connectDS.process(
                //                          第一条流的数据类型  第二条流的数据类型  两条流最终处理后的数据类型
                //                          主流                广播流
                new BroadcastProcessFunction<JSONObject,      TableProcessDim, Tuple2<JSONObject, TableProcessDim>>() {


                    @Override
                    //处理主流业务数据    根据维度表到广播流中读取配置信息，判断是否为维度
                    public void processElement(JSONObject db, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {

                        Thread.sleep(1000);
//                        System.out.println("db----------------------->"+db);
                        String table = db.getJSONObject("source").getString("table");
//                        System.out.printf("   table           "+table);
                        ReadOnlyBroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
//                        HeapBroadcastState{stateMetaInfo=RegisteredBroadcastBackendStateMetaInfo{name='mapStateDescriptor', keySerializer=org.apache.flink.api.common.typeutils.base.StringSerializer@ec756bd, valueSerializer=org.apache.flink.api.java.typeutils.runtime.PojoSerializer@9efb2629, assignmentMode=BROADCAST}, backingMap={}, internalMapCopySerializer=org.apache.flink.api.common.typeutils.base.MapSerializer@691ea70c}
//                        System.out.println("---------------->"+broadcastState);
                        TableProcessDim tableProcessDim = null;

//                        if ((tableProcessDim=broadcastState.get(table))!=null
//                               ){
//                            JSONObject after = db.getJSONObject("after");
//                            JSONObject jsonObject = deleteNotNeedColumns(after, tableProcessDim.getSinkColumns());
//                            String op = db.getString("op");
//                            after.put("op",op);
//                            out.collect(Tuple2.of(jsonObject,tableProcessDim));
//                        }

//||(tableProcessDim=configMap.get(table))!=null
                        if ((tableProcessDim=broadcastState.get(table))!=null){
//                            System.out.printf("==========tableProcessDim====="+tableProcessDim);
                            JSONObject dataJsonObj = db.getJSONObject("after");

                            //在向下游传递数据前，过滤掉不需要传递的属性
                            String sinkColumns = tableProcessDim.getSinkColumns();

                            deleteNotNeedColumns(dataJsonObj, sinkColumns);

                            //在向下游传递数据前，补充对维度数据的操作类型属性
                            String op = db.getString("op");
                            dataJsonObj.put("type", op);

                            out.collect(Tuple2.of(dataJsonObj, tableProcessDim));
                        }


                    }

                    @Override
                    //processBroadcastElement:处理广播流配置信息    将配置数据放到广播状态中
                    public void processBroadcastElement(TableProcessDim tp, BroadcastProcessFunction<JSONObject, TableProcessDim, Tuple2<JSONObject, TableProcessDim>>.Context ctx, Collector<Tuple2<JSONObject, TableProcessDim>> out) throws Exception {
                        String op = tp.getOp();
                        BroadcastState<String, TableProcessDim> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)){

                            //从配置表中删除了一条数据，将对应的配置信息也从广播状态中
                            broadcastState.remove(sourceTable);
//                            configMap.remove(sourceTable);
                        }else {
                            //对配置表进行了读取、添加或者更新操作，将最新的配置信息放到广播状态中
                            broadcastState.put(sourceTable,tp);
//                            configMap.put(sourceTable,tp);
                        }

                    }
                }
        );

        //TODO 11.将维度数据同步到Hbase 表中
//        ({"activity_name":"TCL全场9折","start_time":1642035714000,"create_time":1653609600000,"activity_type":"3103","activity_desc":"TCL全场9折","end_time":1687132800000,"id":4,"type":"r"}
//        ,TableProcessDim(sourceTable=activity_info, sinkTable=dim_activity_info, sinkColumns=id,activity_name,activity_type,activity_desc,start_time,end_time,create_time, sinkFamily=info, sinkRowKey=id, op=r))//
//        TableProcessDim(sourceTable=activity_info, sinkTable=dim_activity_info, sinkColumns=id,activity_name,activity_type,activity_desc,start_time,end_time,create_time, sinkFamily=info, sinkRowKey=id, op=r))
//        dimDS.print("---------------->>>>>");

        dimDS.addSink(new RichSinkFunction<Tuple2<JSONObject, TableProcessDim>>() {


            private Connection connection=null;

            @Override
            public void open(Configuration parameters) throws Exception {

                connection=HBaseUtil.getHBaseConnection();

            }


            @Override
            public void close() throws Exception {

                HBaseUtil.closeHBaseConnection(connection);

            }

            @Override
            public void invoke(Tuple2<JSONObject, TableProcessDim> tup, Context context) throws Exception {

                JSONObject json0bj = tup.f0;
                TableProcessDim tableProcessDim = tup.f1;
                String type = json0bj.getString(  "type");
                json0bj.remove( "type");
//                System.out.println("-------------"+tableProcessDim);
                //获取操作的HBase表的表名
                String sinkTable = tableProcessDim.getSinkTable();

                //获取rowkey
                String rowKey = json0bj.getString(tableProcessDim.getSinkRowKey());
                //判断对业务数据库维度表进行了什么操作
                if("d".equals(type)){
                //从业务数据库维度表中做了删除操作 需要将HBase维度表中对应的记录也删除掉
                    HBaseUtil.delRow(connection, Constant.HBASE_NAMESPACE, sinkTable, rowKey);
                }else {
                    //如果不是delete,可能的类型有insert、update、bootstrap-insert,上述操作对应的都是向HBase表中put数据
                    String sinkFamily = tableProcessDim.getSinkFamily();
                    HBaseUtil.putRow(connection, Constant.HBASE_NAMESPACE, sinkTable, rowKey, sinkFamily, json0bj);


                }
            }
        });



        env.execute();

    }





















//    private static JSONObject deleteNotNeedColumns(JSONObject dataJsonObj,String sinkColumns){
//        JSONObject jsonObject = new JSONObject();
//        String[] split = sinkColumns.split(",");
//        for (String s : split) {
//            jsonObject.put(s,dataJsonObj.getString(s));
//        }
//        return jsonObject;
//
//
//    }

    private static void deleteNotNeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));

    }

}
//



