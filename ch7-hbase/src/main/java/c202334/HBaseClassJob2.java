package c202334;
import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBaseClassJob2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        ResultScanner rs = table.getScanner(scan);
        HashSet<String> famNames = new HashSet<>();
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
            famNames.add(name.substring(0,1));
        }
        admin.close();
        conn.close();
        System.out.println(famNames);
    }
}
