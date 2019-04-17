package com.realtime.app.binlog;

import com.realtime.app.hbase.HBaseUtil;

import java.io.IOException;
import java.util.HashMap;

public class CreateHBaseTableForTableSchema {

    public static void create(){
        HashMap<String, TableSchemaStore.TableSchema> allTableSchema = TableSchemaStore.getAllTableSchema();
        for(String tableName:allTableSchema.keySet()){
            try {
                HBaseUtil.createTable(tableName,"cf");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
