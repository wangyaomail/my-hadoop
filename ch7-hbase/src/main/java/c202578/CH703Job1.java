package c202578;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CH703Job1 extends CH701HBaseBase{

    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Result result = null;

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"));
        ResultScanner scanner = table.getScanner(scan);
        int boyCount = 0, girlCount = 0;
        for (Result res : scanner) {
            String resString = Bytes.toString(res.getValue(Bytes.toBytes("data"), Bytes.toBytes("gender")));
            if (resString.equals("男")) {
                boyCount++;
            } else {
                girlCount++;
            }
        }
        System.out.println("男生人数" + boyCount);
        System.out.println("女生人数" + girlCount);
        conn.close();



    }
    public static void main(String[] args) throws IOException {
        new CH703Job1().run();
    }
}
