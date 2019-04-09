package com.realtime.app.demo;

import com.realtime.app.hbase.HBaseUtil;

import java.util.List;

public class HBaseDemo {
    public static void main(String[] args) {
        try {
            HBaseUtil.init();
            HBaseUtil.createTable("user_table", new String[]{"information", "contact"});
            User user = new User("001", "xiaoming", "123456", "man", "20", "13355550021", "1232821@csdn.com");
            HBaseUtil.insertData("user_table", user);
            User user2 = new User("002", "xiaohong", "654321", "female", "18", "18757912212", "214214@csdn.com");
            HBaseUtil.insertData("user_table", user2);
            List<User> list = HBaseUtil.getAllData("user_table");
            System.out.println("--------------------插入两条数据后--------------------");
            for (User user3 : list) {
                System.out.println(user3.toString());
            }
            System.out.println("--------------------获取原始数据-----------------------");
            HBaseUtil.getNoDealData("user_table");
            System.out.println("--------------------根据rowKey查询--------------------");
            User user4 = HBaseUtil.getDataByRowKey("user_table", "user-001");
            System.out.println(user4.toString());
            System.out.println("--------------------获取指定单条数据-------------------");
            String user_phone = HBaseUtil.getCellData("user_table", "user-001", "contact", "phone");
            System.out.println(user_phone);
            User user5 = new User("test-003", "xiaoguang", "789012", "man", "22", "12312132214", "856832@csdn.com");
            HBaseUtil.insertData("user_table", user5);
            List<User> list2 = HBaseUtil.getAllData("user_table");
            System.out.println("--------------------插入测试数据后--------------------");
            for (User user6 : list2) {
                System.out.println(user6.toString());
            }
            HBaseUtil.deleteByRowKey("user_table", "user-test-003");
            List<User> list3 = HBaseUtil.getAllData("user_table");
            System.out.println("--------------------删除测试数据后--------------------");
            for (User user7 : list3) {
                System.out.println(user7.toString());
            }

            HBaseUtil.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
