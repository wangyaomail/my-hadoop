package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScan {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("students"));

        Scan scan = new Scan();
//        scan.setStartRow(Bytes.toBytes("3"));
//        scan.setLimit(1);
        scan.setFilter(new RowFilter(
                CompareOperator.GREATER,
                new BinaryComparator(Bytes.toBytes("RB0099988"))));

        ResultScanner results = table.getScanner(scan);
        for(Result rs:results){
            String name = Bytes.toString(rs.getValue(Bytes.toBytes("data"),
                    Bytes.toBytes("score")));
            System.out.println(name);
        }


        conn.close();

    }
}
