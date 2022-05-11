package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class CH704GetAndScan extends CH701HBaseBase {
    TableName tableName = TableName.valueOf("students");

    @Override
    public void run() throws IOException {
        Table table = conn.getTable(tableName);

        {
            Get get = new Get(Bytes.toBytes("3"));
            get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
            Result result = table.get(get);

            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("gender"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("home"))));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("dorm"))));
        }
        {
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                System.out.print(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"))));
                System.out.print(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("gender"))));
                System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            }
        }

        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH704GetAndScan().run();
    }
}
