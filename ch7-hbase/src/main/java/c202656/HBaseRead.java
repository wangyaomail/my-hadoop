package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseRead {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

//        Get get = new Get(Bytes.toBytes("1"));
//        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
//        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"));
//        Table table  = conn.getTable(TableName.valueOf("students"));
//        Result rs = table.get(get);
//        System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
//        System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("home"))));


        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes("3"));
//        scan.setStopRow(Bytes.toBytes("5"));
        Table table = conn.getTable(TableName.valueOf("students"));
//        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        ResultScanner sc = table.getScanner(scan);
        scan.setFilter(new RowFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes("2"))));
        for (Result rs : sc) {
            System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("home"))));
        }


        conn.close();
    }
}
