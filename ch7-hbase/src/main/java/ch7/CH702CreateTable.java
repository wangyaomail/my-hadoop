package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;
import java.util.Arrays;

public class CH702CreateTable extends CH701HBaseBase {
    TableName tableName = TableName.valueOf("students");

    @Override
    public void run() throws IOException {
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build();
        builder.setColumnFamily(familyDescriptor);
        TableDescriptor tableDescriptor = builder.build();
        admin.createTable(tableDescriptor);
        System.out.println(Arrays.asList(admin.listTableNames()));
        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH702CreateTable().run();
    }
}
