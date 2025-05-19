package c202556;

import c202578.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Ch701DeleteTest extends CH701HBaseBase {
    public void run() throws IOException {
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
// 删除rowkey为1的学生的所有数据
        Delete delete = new Delete(Bytes.toBytes("1"));
        table.delete(delete);
// 仅删除rowkey为2的学生的宿舍号
        delete = new Delete(Bytes.toBytes("2"));
        delete.addColumn(Bytes.toBytes("data"), Bytes.toBytes("dorm"));
        table.delete(delete);
    }

    public static void main(String[] args) throws IOException {
        new Ch701DeleteTest().run();
    }
}
