package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBase3Score {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);


        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf("students"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        ResultScanner sc = table.getScanner(scan);
//        scan.setFilter(
//                new QualifierFilter(
//                        CompareOperator.EQUAL,
//                        new BinaryComparator(Bytes.toBytes("name")))
//        );
//        scan.setLimit(10);
        int sum=0,count=0;
        for (Result rs : sc) {
            int score = Integer.parseInt(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("score"))));
            sum+=score;
            count++;
        }
        System.out.println(sum/count);


        conn.close();
    }
}
