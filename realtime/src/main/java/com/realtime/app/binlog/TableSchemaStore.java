package com.realtime.app.binlog;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

public class TableSchemaStore {
    private static HashMap<String, TableSchema> tableSchema = new HashMap<>();

    private HashMap<String, TableSchema> init() {
        tableSchema.put("t_tc_project", new TableSchema("t_tc_project", "project_id", "update_time"));
        tableSchema.put("t_tc_project_invest_order", new TableSchema("t_tc_project_invest_order", "id", "update_time"));
        tableSchema.put("t_tc_project_wait_publish", new TableSchema("t_tc_project_wait_publish", "project_id", "create_time"));
        tableSchema.put("t_tc_refund_order", new TableSchema("t_tc_refund_order", "id", "update_time"));
        tableSchema.put("t_tc_refund_order_detail_biz", new TableSchema("t_tc_refund_order_detail_biz", "id", "update_time"));
        tableSchema.put("t_tc_user_invest_config", new TableSchema("t_tc_user_invest_config", "id", "update_time"));
        tableSchema.put("t_tc_user_invest_prize_config", new TableSchema("t_tc_user_invest_prize_config", "id", "update_time"));


        tableSchema.put("t_ac_account_base", new TableSchema("t_ac_account_base", "id", "update_time"));
        tableSchema.put("t_ac_recharge_order", new TableSchema("t_ac_recharge_order", "id", "update_time"));

        tableSchema.put("t_uc_basic_info", new TableSchema("t_uc_basic_info", "user_id", "update_time"));
        tableSchema.put("t_uc_ext_info", new TableSchema("t_uc_ext_info", "user_id", "update_time"));
        tableSchema.put("t_uc_identity_info", new TableSchema("t_uc_identity_info", "user_id", "update_time"));
        tableSchema.put("t_uc_invite_register", new TableSchema("t_uc_invite_register", "user_id", "create_time"));

        tableSchema.put("tab_userprize", new TableSchema("tab_userprize", "id", "updateTime"));

        return tableSchema;
    }

    /**
     * 获得所有表信息
     * @return
     */
    public static HashMap<String, TableSchema> getAllTableSchema(){
        loadData();
        return tableSchema;
    }

    /**
     * 初始化表信息
     */
    public static void loadData(){
        if(TableSchemaStore.tableSchema.size()==0){
            synchronized (TableSchemaStore.tableSchema){
                if(TableSchemaStore.tableSchema.size()==0){
                    TableSchemaStore.tableSchema = new TableSchemaStore().init();
                }
            }
        }
    }

    public static String getTablePrimaryKeyName(String tableName) {
        loadData();
        //分表带数字后缀
        if (tableName.indexOf("tab_userprize") != -1) {
            tableName = tableSchema.get("tab_userprize").primarykeyName;
        }
        return tableSchema.get(tableName).primarykeyName;
    }

    public static String getTableUpdateTimeKeyName(String tableName) {
        loadData();
        //分表带数字后缀
        if (tableName.indexOf("tab_userprize") != -1) {
            tableName = tableSchema.get("tab_userprize").updateTime;
        }
        return tableSchema.get(tableName).updateTime;
    }

    public static Tuple2<String,String> getTableUpdateTimeAndPrimaryKeyKeyName(String tableName) {
        loadData();
        Tuple2<String,String> tp = new Tuple2<>();
        //分表带数字后缀
        if (tableName.indexOf("tab_userprize") != -1) {
            tableName = tableSchema.get("tab_userprize").updateTime;
        }
        tp.f0 = tableSchema.get(tableName).updateTime;

        //分表带数字后缀
        if (tableName.indexOf("tab_userprize") != -1) {
            tableName = tableSchema.get("tab_userprize").primarykeyName;
        }
        tp.f1 = tableSchema.get(tableName).primarykeyName;
        return tp;
    }



    class TableSchema {
        private String tableName;
        private String primarykeyName;
        private String updateTime;

        public TableSchema(String tableName, String primarykeyName, String updateTime) {
            this.tableName = tableName;
            this.primarykeyName = primarykeyName;
            this.updateTime = updateTime;
        }
    }
}
