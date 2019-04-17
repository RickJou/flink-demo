package com.realtime.app.binlog;

import com.alibaba.fastjson.JSONArray;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class BinlogDmlPo {
    private JSONArray records;
    private String databaseName;
    private String tableName;
    private String primaryKeyName;
}
