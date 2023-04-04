package ch7;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CH705DeleteTest extends CH701HBaseBase {
    TableName tableName = TableName.valueOf("students");
    @Override
    public void run() throws IOException {
        Table table = conn.getTable(tableName);
        {
            Delete delete = new Delete(Bytes.toBytes("4"));
            delete.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"));
            table.delete(delete);
        }
        conn.close();
    }
    public static void main(String[] args) throws IOException {
        new CH705DeleteTest().run();
    }
}
