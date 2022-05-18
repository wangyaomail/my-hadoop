package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;

public class CH713MaxMinScore extends CH701HBaseBase {
    @Override
    public void run() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        Table table = conn.getTable(TableName.valueOf("students"));
        ResultScanner resultScanner = table.getScanner(scan);

        int maxScore = Integer.MIN_VALUE, minScore = Integer.MAX_VALUE;
        for (Result result : resultScanner) {
            String scoreStr = Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("score")));
            Integer score = Integer.parseInt(scoreStr);
            if (score > maxScore) {
                maxScore = score;
            }
            if (score < minScore) {
                minScore = score;
            }
        }

        System.out.println("最高分："+maxScore);
        System.out.println("最低分："+minScore);
        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH713MaxMinScore().run();

    }
}
