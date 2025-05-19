package c202578;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;

public class CH703Job2 extends CH701HBaseBase{

    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Result result = null;

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        ResultScanner scanner = table.getScanner(scan);
        HashSet<String> names = new HashSet<>();
        for (Result res : scanner) {
            String name = Bytes.toString(res.getValue(Bytes.toBytes("data"),
                    Bytes.toBytes("name")));
            names.add(name.substring(0,1));
        }
        System.out.println("names: " + names);
        conn.close();
    }
    public static void main(String[] args) throws IOException {
        new CH703Job2().run();
    }
}
