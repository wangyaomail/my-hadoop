package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBase3Name {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);


        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf("students"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        ResultScanner sc = table.getScanner(scan);
//        scan.setFilter(
//                new QualifierFilter(
//                        CompareOperator.EQUAL,
//                        new BinaryComparator(Bytes.toBytes("name")))
//        );
//        scan.setLimit(10);
        HashSet<String> set = new HashSet<>();
        for (Result rs : sc) {
            String name = Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
            set.add(name.split("")[0]);
        }
        System.out.println(set.size());


        conn.close();
    }
}
