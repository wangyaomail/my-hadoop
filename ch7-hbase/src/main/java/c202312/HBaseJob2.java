package c202312;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBaseJob2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        ResultScanner rsc = table.getScanner(scan);
        HashSet<String> nameSet = new HashSet<>();
        for (Result r : rsc) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
            nameSet.add(name.substring(0,1));
        }
        System.out.println(nameSet);
        admin.close();
        conn.close();
    }
}
