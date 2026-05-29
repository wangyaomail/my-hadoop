package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBasePut {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        Table table = conn.getTable(TableName.valueOf("students"));
        Put put = new Put(Bytes.toBytes("8"));
        put.addColumn(Bytes.toBytes("data"),
                Bytes.toBytes("name"),
                1l,
                Bytes.toBytes("zhangsan"));
        put.addColumn(Bytes.toBytes("data"),
                Bytes.toBytes("gender"),
                1l,
                Bytes.toBytes("male"));
        put.addColumn(Bytes.toBytes("data"),
                Bytes.toBytes("birthday"),
                1l,
                Bytes.toBytes("2003-04-06"));
        put.addColumn(Bytes.toBytes("data"),
                Bytes.toBytes("home"),
                1l,
                Bytes.toBytes("kaifeng"));
        put.addColumn(Bytes.toBytes("data"),
                Bytes.toBytes("dorm"),
                1l,
                Bytes.toBytes("1#101"));
        table.put(put);

//        List<Put> putList = new ArrayList<Put>();
//        put = new Put(Bytes.toBytes("5"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("male"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-10-11"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("anyang"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("1#101"));
//        putList.add(put);
//        put = new Put(Bytes.toBytes("6"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("sunzi"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2003-09-15"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("shangqiu"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
//        putList.add(put);
//        put = new Put(Bytes.toBytes("7"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhouba"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes("female"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes("2002-07-08"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes("zhoukou"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"), Bytes.toBytes("2#101"));
//        putList.add(put);
//        table.put(putList);


        admin.close();
        conn.close();
    }
}
