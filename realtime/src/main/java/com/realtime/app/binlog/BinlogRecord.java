package com.realtime.app.binlog;

import com.alibaba.fastjson.JSONObject;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BinlogRecord {
    private JSONObject record;
    private String databaseName;
    private String tableName;
    private String primaryKeyName;
}
