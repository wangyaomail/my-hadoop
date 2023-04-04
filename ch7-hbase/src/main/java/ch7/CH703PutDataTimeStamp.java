package ch7;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class CH703PutDataTimeStamp extends CH701HBaseBase {
    TableName tableName = TableName.valueOf("students2");
    @Override
    public void run() throws IOException {
        Table table = conn.getTable(tableName);
        {
            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("name"),
                    100,
                    Bytes.toBytes("zhangsan"));
            table.put(put);
        }
        conn.close();
    }
    public static void main(String[] args) throws IOException {
        new CH703PutDataTimeStamp().run();
    }
}
