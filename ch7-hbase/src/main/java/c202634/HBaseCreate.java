package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class HBaseCreate {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("s3");

//// 创建学生表
//        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
//        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build();
//        builder.setColumnFamily(familyDescriptor);
//        TableDescriptor tableDescriptor = builder.build();
//        admin.createTable(tableDescriptor);
//        System.out.println("当前的table："+ Arrays.asList(admin.listTableNames()));
// 删除学生表
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("当前的table："+Arrays.asList(admin.listTableNames()));



        admin.close();
        conn.close();

    }
}
