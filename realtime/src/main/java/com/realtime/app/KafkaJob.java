package com.realtime.app;

import com.alibaba.fastjson.JSONObject;
import com.realtime.app.binlog.BinlogCountTrigger;
import com.realtime.app.binlog.BinlogDeserializationSchema;
import com.realtime.app.binlog.BinlogDmlPo;
import com.realtime.app.po.BaseTablePo;
import com.realtime.app.po.T_tc_project_invest_order;
import com.realtime.app.util.BeanUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.util.Properties;

public class KafkaJob {

    private static Logger log = Logger.getLogger(KafkaJob.class);

    public static void main(String[] args) {
        try {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            /*检查点相关,存储相关配置在flink.yml中进行了统一配置*/
            env.enableCheckpointing(10 * 1000); // 10秒保存一次检查点
            CheckpointConfig config = env.getCheckpointConfig();
            config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//取消程序时保留检查点
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//恰好一次语义


            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);// 使用事件时间
            env.setParallelism(1);//并行度

            /*kafka属性*/
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "192.168.21.91:9090, 192.168.21.92:9090, 192.168.21.93:9090");
            properties.setProperty("group.id", "flink_default_group");
            properties.setProperty("auto.offset.reset", "earliest");
            properties.setProperty("enable.auto.commit", "false");
            properties.setProperty("group.max.session.timeout.ms", "300000");
            properties.setProperty("session.timeout.ms", "9000");
            properties.setProperty("heartbeat.interval.ms", "3000");

            FlinkKafkaConsumer<BinlogDmlPo> consumer = new FlinkKafkaConsumer<>(
                    //java.util.regex.Pattern.compile("(^t_ac_\\S*)|(^t_tc_\\S*)|(^t_uc_\\S*)"),//三个库中的表前缀匹配topic名称
                    java.util.regex.Pattern.compile("t_tc_project_invest_order"),
                    new BinlogDeserializationSchema(),//自定义反序列化器
                    properties);


            /*kafka record 读取模式*/
            consumer.setStartFromEarliest();     // 从最早记录开始读取

            //consumer.setStartFromLatest();       // 从最新记录开始读取
            //consumer.setStartFromTimestamp(...); // 从指定时间戳(毫秒)开始
            //consumer.setStartFromGroupOffsets(); // 默认使用此项,读取保存在kafka队列中group 的分区的偏移量 (会丢数据,kafka自行维护的offset不可靠)
            /*Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
            specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 0L);
            specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 10L);
            specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 20L);
            consumer.setStartFromSpecificOffsets(specificStartOffsets);//从消费组指定offset位置开始读取record ,the default behaviour
            */


            /*增加时间和水位设置*/
            //consumer.assignTimestampsAndWatermarks(new KafkaJob().getBinlogAssignerWithPunctuatedWatermarks());


            DataStream<BinlogDmlPo> stream = env.addSource(consumer);

            //将源数据保存到hbase,将create_time和update_time索引数据保存到redis

            //拆分一个record中包含多个sql
            DataStream<T_tc_project_invest_order> RecordStream = stream.flatMap(new FlatMapFunction<BinlogDmlPo, T_tc_project_invest_order>() {
                @Override
                public void flatMap(BinlogDmlPo binlogDmlPo, Collector<T_tc_project_invest_order> collector) {
                    for (int i = 0; i < binlogDmlPo.getRecords().size(); i++) {
                        JSONObject rec = binlogDmlPo.getRecords().getJSONObject(i);
                        T_tc_project_invest_order po = BeanUtil.map2Bean(rec.getInnerMap(), T_tc_project_invest_order.class);
                        po.getUpdate_day();
                        po.getCreate_day();
                        collector.collect(po);
                    }
                }
            }).assignTimestampsAndWatermarks(new BaseTablePo().getCommonRecordAssignerWithPunctuatedWatermarks("create_time"));//水位时间戳


            //按同天的同一行记录进行分组
            WindowedStream<T_tc_project_invest_order, String, TimeWindow> window =
                    RecordStream.keyBy((KeySelector<T_tc_project_invest_order, String>) po -> po.getId())
                            .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(0)))//事件时间翻滚窗口一天一窗口
                            //.trigger(ContinuousEventTimeTrigger.of(Time.seconds(15)))//5秒钟触发一次窗口计算
                            //.trigger(CountTrigger.of(1))
                            .trigger(BinlogCountTrigger.create(1))
                            .allowedLateness(Time.days(1));//允许延时1天

            //微批数据去重(执行reduce取此批数据中,同id情况下update_time最大的一条)
            DataStream<T_tc_project_invest_order> windowStream =
                    window.reduce((ReduceFunction<T_tc_project_invest_order>) (t1, t2) -> {
                        return t1.getLong_update_time() > t2.getLong_update_time() ? t1 : t2;
                    });

            StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

            windowStream.print();

            Table dynamic_t_tc_project_invest_order = tableEnv.fromDataStream(windowStream, "id,update_time,status,create_day,amount,deadline,deadline_unit");
            tableEnv.registerTable("dynamic_t_tc_project_invest_order", dynamic_t_tc_project_invest_order);


            //String sql = "select id,update_time,status,create_day,amount,deadline,deadline_unit from dynamic_t_tc_project_invest_order group by id,update_time,status,create_day,amount,deadline,deadline_unit";

            String sql = "select create_day,sum(amount),deadline,deadline_unit from dynamic_t_tc_project_invest_order group by create_day,deadline,deadline_unit";


            Table dynamictTable = tableEnv.sqlQuery(sql);



            DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(dynamictTable, Row.class);
            resultStream.print();


            //HBaseTableSink.emitDataStream(resultStream);


            env.execute("Flink Streaming Java API Skeleton");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
