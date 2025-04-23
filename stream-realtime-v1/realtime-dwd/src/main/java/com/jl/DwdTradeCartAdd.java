package com.jl;

import com.jl.util.MyKafkaUtil;
import com.jl.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.jl.DwdTradeCartAdd
 * @Author jia.le
 * @Date 2025/4/20 18:49
 * @description: 1
 */


// TODO: 2025/4/20   将架构表写入kfk
public class DwdTradeCartAdd {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        // 从kafka的topic_db主题中读取数据创建动态表       ---kafka连接器

        tableEnv.executeSql("create table topic_db(  " +
                "  `before` map<string,string>,  " +
                "  `after` map<string,string>,  " +
                "  `source` map<string,string>,  " +
                "  `op` string,  " +
                "  `ts_ms` BIGINT,  " +
                "  proc_time as proctime()  " +
                " ) "+  MyKafkaUtil.getKafkaDDL2("topic_db","one1"));


        //TODO 过滤出加购数据  table='cart_info' type='insert' 、type = 'update' 并且修改的是加购商品的数量，修改后的值大于修改前的值



        Table cartInfo = tableEnv.sqlQuery("select \n" +
                "   `after`['id'] id,\n" +
                "   `after`['user_id'] user_id,\n" +
                "   `after`['sku_id'] sku_id,\n" +
                "   if(op='c',`after`['sku_num'], CAST((CAST(after['sku_num'] AS INT) - CAST(`before`['sku_num'] AS INT)) AS STRING)) sku_num, \n" +
                "   ts_ms\n" +
                "from topic_db \n" +
                "where `source`['table']='cart_info' \n" +
                "and (\n" +
                "    op = 'c'\n" +
                "    or\n" +
                "    (op='u' and `before`['sku_num'] is not null and (CAST(`after`['sku_num'] AS INT) >= CAST(`before`['sku_num'] AS INT)))\n" +
                ")");

        cartInfo.execute().print();
        tableEnv.createTemporaryView("cartInfo",cartInfo);


        tableEnv.executeSql(" create table dwd_trade_cart_add(\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    sku_num string,\n" +
                "    ts_ms bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                " )" + SQLUtil.getUpsertKafkaDDL("dwd_trade_cart_add")  );
        //写入
        cartInfo.executeInsert("dwd_trade_cart_add");

//topic_db   的  json 字符串
/*
        {
  "before": {
    "id": 815,
    "user_id": "217",
    "sku_id": 21,
    "cart_price": "BQis",
    "sku_num": 2,
    "img_url": null,
    "sku_name": "小米电视4A 70英寸 4K超高清 HDR 二级能效 2GB+16GB L70M5-4A 内置小爱 智能网络液晶平板教育电视",
    "is_checked": null,
    "create_time": 1744145997000,
    "operate_time": 1744146001000,
    "is_ordered": 1,
    "order_time": 1744145998000
  },
  "after": {
    "id": 815,
    "user_id": "217",
    "sku_id": 21,
    "cart_price": "BQis",
    "sku_num": 2,
    "img_url": null,
    "sku_name": "小米电视4A 70英寸 4K超高清 HDR 二级能效 2GB+16GB L70M5-4A 内置小爱 智能网络液晶平板教育电视",
    "is_checked": null,
    "create_time": 1744145997000,
    "operate_time": 1744146002000,
    "is_ordered": 1,
    "order_time": 1744146002000
  },
  "source": {
    "version": "1.9.7.Final",
    "connector": "mysql",
    "name": "mysql_binlog_source",
    "ts_ms": 1744981202000,
    "snapshot": "false",
    "db": "realtime_v1",
    "sequence": null,
    "table": "cart_info",
    "server_id": 1,
    "gtid": null,
    "file": "mysql-bin.000020",
    "pos": 3588048,
    "row": 0,
    "thread": 319,
    "query": null
  },
  "op": "u",
  "ts_ms": 1744981199721,
  "transaction": null
}
         */


    }
}
