package com.realtime.app.hbase.sink;

import com.alibaba.fastjson.JSONObject;
import com.realtime.app.binlog.BinlogRecord;
import com.realtime.app.binlog.TableSchemaStore;
import com.realtime.app.hbase.HBaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBasePutBinlogRecordsSink extends RichSinkFunction<List<BinlogRecord>> {
    private static Logger log = LoggerFactory.getLogger(HBasePutBinlogRecordsSink.class);

    @Override
    public void open(Configuration parameters) throws Exception {
        HBaseUtil.init();//初始化连接
    }

    @Override
    public void invoke(List<BinlogRecord> records, Context context) throws Exception {
        putRecords(records, "cf");//批量写入
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
}
