package com.jl.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.jl.bean.CartAddUuBean;
import com.jl.function.BeanToJsonStrMapFunction;
import com.jl.utils.DateFormatUtil;
import com.jl.utils.FlinkSinkUtil;
import com.jl.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.jl.DwsTradeCartAddUuWindow
 * @Author jia.le
 * @Date 2025/4/21 14:31
 * @description: DwsTradeCartAddUuWindow
 */
//每日加购独立用户数   实时统计加购UV，判断秒杀、满减等活动是否吸引新用户。
//    每个用户每天可能多次加购商品，但统计 每日独立用户数（UV） 时，需确保每个用户当天只被计数一次。
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
//        精准一次语义         保障数据在故障恢复后不重复、不丢失
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
//        从 dwd_trade_cart_add 主题读取用户加购明细数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_cart_add", "dws_trade_cart_add_uu_window");

        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );

        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
//                KeyedProcessFunction	按用户分组处理，确保状态隔离
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
//                    ValueState 状态	存储每个用户最后一次加购日期
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        //TTL（1天过期）	自动清理超过1天的状态数据  （每日）
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms");
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartUUDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2)));
//                                  通过 Flink 窗口聚合统计 每日加购独立用户数（UV）
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;// 初始化计数器为0
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;// 每收到一条数据，计数器+1
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator; // 返回最终计数结果
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;// 窗口合并逻辑（示例未实现）
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {

//                        时间信息整合：将窗口起止时间、统计日期与UV值绑定，形成完整统计记录。
                        Long cartUUCt = values.iterator().next();
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );
//JSON格式转换（6
        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction<>());

        operator.print();

        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        env.execute("DwsTradeCartAddUuWindow");
    }
}
