package c202334;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseGetAndScan {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("students");
//        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
//        ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("data".getBytes()).build();
//        builder.setColumnFamily(familyDescriptor);
//        TableDescriptor tableDescriptor = builder.build();
//        admin.createTable(tableDescriptor);
        Table table = conn.getTable(tableName);
        Put put = new Put(Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        table.put(put);
        put = new Put(Bytes.toBytes("zhangfei"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangfei"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        table.put(put);
        put = new Put(Bytes.toBytes("zhangyide"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangyide"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        table.put(put);
        List<Put> putList = new ArrayList<Put>();
        put = new Put(Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-10-11"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("anyang"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        putList.add(put);
        put = new Put(Bytes.toBytes("wangwu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-09-15"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("shangqiu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
        putList.add(put);
        put = new Put(Bytes.toBytes("zhaoliu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhaoliu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2002-07-08"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("zhoukou"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
        putList.add(put);
        table.put(putList);
//        Get get = new Get(Bytes.toBytes("1"));
//        get.addColumn(Bytes.toBytes("data"),Bytes.toBytes("name"));
//        get.addColumn(Bytes.toBytes("data"),Bytes.toBytes("gender"));
//        get.addColumn(Bytes.toBytes("data"),Bytes.toBytes("birthday"));
//        Result result = table.get(get);
//        System.out.println(Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("data"),Bytes.toBytes("name")))));
//        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"),Bytes.toBytes("gender"))));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        scan.withStartRow(Bytes.toBytes("zhang"));
        scan.withStopRow(Bytes.toBytes("zhanh"));
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
        }
        admin.close();
        conn.close();
    }
}