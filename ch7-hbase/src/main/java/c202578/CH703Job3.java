package c202578;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;

public class CH703Job3 extends CH701HBaseBase{

    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Result result = null;

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        ResultScanner scanner = table.getScanner(scan);
        int maxValue = Integer.MIN_VALUE;
        int minValue = Integer.MAX_VALUE;
        for (Result res : scanner) {
            int score = Integer.parseInt(Bytes.toString(res.getValue(Bytes.toBytes("data"),
                    Bytes.toBytes("score"))));
            if(score > maxValue) {
                maxValue = score;
            }
            if(score < minValue) {
                minValue = score;
            }
        }
        System.out.println(minValue+":" + maxValue);
        conn.close();
    }
    public static void main(String[] args) throws IOException {
        new CH703Job3().run();
    }
}
