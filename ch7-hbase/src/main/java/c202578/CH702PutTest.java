package c202578;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CH702PutTest extends CH701HBaseBase{

    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);

        Put put = new Put(Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        table.put(put);
        List<Put> putList = new ArrayList<Put>();
        put = new Put(Bytes.toBytes("2"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-10-11"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("anyang"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
        putList.add(put);
        put = new Put(Bytes.toBytes("3"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-09-15"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("shangqiu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
        putList.add(put);
        put = new Put(Bytes.toBytes("4"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhaoliu"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2002-07-08"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("zhoukou"));
        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
        putList.add(put);
        table.put(putList);


    }
    public static void main(String[] args) throws IOException {
        new CH702PutTest().run();
    }
}
