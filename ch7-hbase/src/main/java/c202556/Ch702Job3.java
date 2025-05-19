package c202556;

import c202578.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;

public class Ch702Job3 extends CH701HBaseBase {
    public void run() throws IOException {
        Table table = conn.getTable(TableName.valueOf("students"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
//        scan.setFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("男"))));
        ResultScanner scanner = table.getScanner(scan);
        int minScore = Integer.MAX_VALUE;
        int maxScore = Integer.MIN_VALUE;
        for (Result res : scanner) {
            int score = Integer.parseInt(Bytes.toString(res.getValue(Bytes.toBytes("data"),
                    Bytes.toBytes("score"))));
            if (score < minScore) {
                minScore = score;
            }
            if (score > maxScore) {
                maxScore = score;
            }
        }
        System.out.println(minScore+":"+maxScore);
    }

    public static void main(String[] args) throws IOException {
        new Ch702Job3().run();
    }
}
