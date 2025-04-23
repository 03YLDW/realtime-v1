package com.jl;

import com.jl.util.MyKafkaUtil;
import com.jl.util.SQLUtil;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * @Package com.jl.DwdinteractionCommentinfo
 * @Author jia.le
 * @Date 2025/4/18 18:53
 * @description: 1
 */


// TODO: 2025/4/20 从topic_db自中将 评论表拿出  然后从hbase 中将 字典表拿出   将评论表和字典表关联  写入kfk dwd_interaction_comment_info
public class DwdinteractionCommentinfo {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度，
        env.setParallelism(4);
        // flink sql初始化
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //从kfk中拿到topic_db
        tableEnv.executeSql(" create table topic_db(\n" +
                "    `after` map<string,string>,\n" +
                "    `source` map<string,string>,\n" +
                "    `op` string,\n" +
                "    `ts_ms` BIGINT,\n" +
                "    proc_time as proctime()\n ) "+MyKafkaUtil.getKafkaDDL2("topic_db","one"));

//        tableEnv.executeSql("select  * from  topic_db ").print();



        //评论表   comment_info
        Table commentInfo = tableEnv.sqlQuery("select \n" +
                "`after`['id'] id,\n" +
                "`after`['user_id'] user_id,\n" +
                "`after`['sku_id'] sku_id,\n" +
                "`after`['appraise'] appraise,\n" +  //评论级别
                "`after`['comment_txt'] comment_txt,\n" +
                "`ts_ms`,\n" +
                "`proc_time` \n" +
                "from topic_db where `source`['table']='comment_info'");
//        commentInfo.execute().print();

        tableEnv.createTemporaryView("comment_info",commentInfo);

        //从hbase中拿到字典表
        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_code string,\n" +
                " info ROW<dic_name STRING>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                " ) " + SQLUtil.getHBaseDDL("realtime_v1","dim_base_dic"));

//        tableEnv.executeSql("select * from base_dic").print();


        Table jointable = tableEnv.sqlQuery("select " +
                "id ," +
                "user_id, " +
                "sku_id, " +
                "appraise, " +
                "dic.dic_name appraise_name," +
                "comment_txt," +
                " ts_ms   " +
                "  from  comment_info as c  JOIN " +
                " base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic\n" +
                "    ON dic.dic_code = c.appraise  ");

        tableEnv.executeSql("create table   dwd_interaction_comment_info ( " +
                " id string  , " +
                " user_id string ," +
                " sku_id string ," +
                " appraise string ," +
                " appraise_name string ," +
                " comment_txt string ," +
                " ts_ms bigint," +
                "  PRIMARY KEY (id) NOT ENFORCED " +
                ") " + SQLUtil.getUpsertKafkaDDL("dwd_interaction_comment_info"));

            jointable.executeInsert("dwd_interaction_comment_info");
        //  字典表对应的级别    评论级别
    }
}
