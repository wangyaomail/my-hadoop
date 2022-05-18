package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;

public class CH712AllFamilyName extends CH701HBaseBase {
    @Override
    public void run() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        Table table = conn.getTable(TableName.valueOf("students"));
        ResultScanner resultScanner = table.getScanner(scan);
        HashSet<String> familyNames = new HashSet<>();
        for (Result result : resultScanner) {
            String name = Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
            String familyName = name.split("")[0];
            familyNames.add(familyName);
        }
        System.out.println(familyNames);
        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH712AllFamilyName().run();

    }
}
