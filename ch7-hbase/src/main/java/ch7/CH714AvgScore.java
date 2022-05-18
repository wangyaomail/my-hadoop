package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CH714AvgScore extends CH701HBaseBase {
    @Override
    public void run() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        Table table = conn.getTable(TableName.valueOf("students"));
        ResultScanner resultScanner = table.getScanner(scan);

        double scoreAdd = 0, scoreCount = 0;
        for (Result result : resultScanner) {
            String scoreStr = Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("score")));
            Integer score = Integer.parseInt(scoreStr);
            scoreAdd+= score;
            scoreCount++;
        }

        System.out.println("平均分："+scoreAdd/scoreCount);
        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH714AvgScore().run();

    }
}
