package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCreate {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
//
//        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TableName.valueOf("bbb"));
//        ColumnFamilyDescriptor familyDescriptor =
//                ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build();
//        builder.setColumnFamily(familyDescriptor);
//        TableDescriptor tableDescriptor = builder.build();
//        admin.createTable(tableDescriptor);
        admin.disableTable(TableName.valueOf("bbb"));
        admin.deleteTable(TableName.valueOf("bbb"));


        admin.close();
        conn.close();
    }
}
