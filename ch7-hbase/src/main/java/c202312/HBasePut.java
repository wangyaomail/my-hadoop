package c202312;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBasePut {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("s2");
        Table table = conn.getTable(tableName);
        long start = System.currentTimeMillis();
//        for(int i=0;i<20000;i++) {
//            Put put = new Put(Bytes.toBytes(i+""));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
//            table.put(put);
//        }

        List<Put> putList = new ArrayList<Put>();
        for(int i=0;i<20000;i++) {
            Put put = new Put(Bytes.toBytes(i+""));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-04-06"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("kaifeng"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
            putList.add(put);
        }
//        put = new Put(Bytes.toBytes("2"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-10-11"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("anyang"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
//        putList.add(put);
//        put = new Put(Bytes.toBytes("3"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-09-15"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("shangqiu"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
//        putList.add(put);
//        put = new Put(Bytes.toBytes("4"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhaoliu"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2002-07-08"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("zhoukou"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
//        putList.add(put);
        table.put(putList);
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        admin.close();
        conn.close();
    }
}
