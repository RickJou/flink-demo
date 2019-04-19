package com.realtime.app.hbase.sink;

import com.alibaba.fastjson.JSONObject;
import com.realtime.app.binlog.BinlogRecord;
import com.realtime.app.binlog.TableSchemaStore;
import com.realtime.app.hbase.HBaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBasePutBinlogRecordsSink extends RichSinkFunction<List<BinlogRecord>> implements CheckpointedFunction {
    private static Logger log = LoggerFactory.getLogger(HBasePutBinlogRecordsSink.class);

    List<BinlogRecord> temp;//用于写入hbase之前留个引用,以便于检查点快照拷贝数据.以及在恢复时,会在此处添加要恢复的元素,以便于后续一起写入.
    private transient static ListState<BinlogRecord> listStage;//检查点状态

    public HBasePutBinlogRecordsSink() {
        this.temp = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        HBaseUtil.init();//初始化连接
    }

    @Override
    public void invoke(List<BinlogRecord> records, Context context) throws Exception {
        temp.addAll(records);
        putRecords(records, "cf");//批量写入habse
        temp.clear();
    }

    private void putRecords(List<BinlogRecord> records, String columnFamily) throws IOException {
        long startTime = System.currentTimeMillis();
        String cf = columnFamily == null ? "cf" : columnFamily;
        String tableName = "";
        List<Put> listput = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            BinlogRecord br = records.get(i);
            tableName = br.getTableName();
            JSONObject rec = br.getRecord();
            Tuple2<String, String> pkAndUpdateTimeKeyName = TableSchemaStore.getTableUpdateTimeAndPrimaryKeyKeyName(br.getTableName());

            String update = rec.getString(pkAndUpdateTimeKeyName.f0);
            String id = rec.getString(pkAndUpdateTimeKeyName.f1);
            String rowKey = update.substring(0, 10) + "-" + id;

            Put put = new Put(rowKey.getBytes());
            for (String key : rec.keySet()) {
                String value = rec.getString(key);
                if (StringUtils.isNotBlank(value)) {
                    put.addColumn(cf.getBytes(), key.getBytes(), value.getBytes());
                }
            }
            listput.add(put);
        }
        HBaseUtil.getConnection().getTable(TableName.valueOf(tableName)).put(listput);
        long endTime = System.currentTimeMillis();
        log.info("hbase put list success,list size:{} time cost: {} ms", records.size(), (endTime - startTime));
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        listStage.clear();
        listStage.addAll(temp);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        //初始化
        ListStateDescriptor<BinlogRecord> descriptor = new ListStateDescriptor("buffered-elements", TypeInformation.of(new TypeHint<ListStateDescriptor>() {}));

        //每个运算符返回一个状态元素列表。整个状态在逻辑上是所有列表的串联。在恢复/重新分发时，列表被平均分成与并行运算符一样多的子列表。每个运算符都获得一个子列表，该子列表可以为空，或包含一个或多个元素。
        listStage = context.getOperatorStateStore().getListState(descriptor);

        //联合重新分配:每个运算符返回一个状态元素列表。整个状态在逻辑上是所有列表的串联。在恢复/重新分配时，每个运算符都会获得完整的状态元素列表。
        //context.getOperatorStateStore().getUnionListState(descriptor);

        if (context.isRestored()) {//恢复状态
            for (BinlogRecord element : listStage.get()) {
                temp.add(element);
            }
        }
    }
}
