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
import java.util.HashSet;

public class Ch702Job2 extends CH701HBaseBase {
    public void run() throws IOException {
        Table table = conn.getTable(TableName.valueOf("students"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
//        scan.setFilter(new ValueFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("男"))));
        ResultScanner scanner = table.getScanner(scan);
        HashSet<String> fnames = new HashSet<>();
        for (Result res : scanner) {
            fnames.add(Bytes.toString(res.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))).substring(0,1));
        }
        System.out.println(fnames);
    }

    public static void main(String[] args) throws IOException {
        new Ch702Job2().run();
    }
}
