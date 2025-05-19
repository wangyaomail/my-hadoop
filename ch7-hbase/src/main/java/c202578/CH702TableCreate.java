package c202578;

import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;
import java.util.Arrays;

public class CH702TableCreate extends CH701HBaseBase{

    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");

        // 删除学生表
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("当前的table："+Arrays.asList(admin.listTableNames()));

        // 创建学生表
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build();
        builder.setColumnFamily(familyDescriptor);
        TableDescriptor tableDescriptor = builder.build();
        admin.createTable(tableDescriptor);
        System.out.println("当前的table："+ Arrays.asList(admin.listTableNames()));
    }
    public static void main(String[] args) throws IOException {
        new CH702TableCreate().run();
    }
}
