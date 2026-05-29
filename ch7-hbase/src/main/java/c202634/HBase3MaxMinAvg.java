package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBase3MaxMinAvg {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("students"));

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
//        scan.setFilter(new RowFilter(
//                CompareOperator.GREATER,
//                new BinaryComparator(Bytes.toBytes("RB0099988"))));

        int max=0,min=100;
        int sum=0,count=0;
        ResultScanner results = table.getScanner(scan);
        for(Result rs:results){
            String scoreStr = Bytes.toString(rs.getValue(
                    Bytes.toBytes("data"),
                    Bytes.toBytes("score")));
            if(scoreStr!=null){
                int score = Integer.parseInt(scoreStr);
                sum += score;
                count++;
                if(max<score){
                    max=score;
                }
                if(min>score){
                    min=score;
                }
            }

        }

        System.out.println((sum/count)+"\t"+max+"\t"+min);

        conn.close();

    }
}
