package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBase2QuChong {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("students"));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
//        scan.setFilter(new RowFilter(
//                CompareOperator.GREATER,
//                new BinaryComparator(Bytes.toBytes("RB0099988"))));

        HashSet<String> set = new HashSet<>();
        ResultScanner results = table.getScanner(scan);
        for(Result rs:results){
            String name = Bytes.toString(rs.getValue(Bytes.toBytes("data"),
                    Bytes.toBytes("name")));
            set.add(name.split("")[0]);
        }

        System.out.println(set.size());


        conn.close();

    }
}
