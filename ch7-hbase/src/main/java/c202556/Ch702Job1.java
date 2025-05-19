package c202556;

import c202578.CH701HBaseBase;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Ch702Job1 extends CH701HBaseBase {
    public void run() throws IOException {
        Table table = conn.getTable(TableName.valueOf("students"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"));
        scan.setFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("男"))));
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result res : scanner) {
            count++;
        }
        System.out.println(count);
    }

    public static void main(String[] args) throws IOException {
        new Ch702Job1().run();
    }
}
