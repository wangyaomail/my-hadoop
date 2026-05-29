package c202634;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseDelete {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

//        Table table = conn.getTable(TableName.valueOf("students"));
//
//        Delete delete = new Delete(Bytes.toBytes("3"));
//        delete.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
//
//        table.delete(delete);

        Admin admin = conn.getAdmin();

        admin.disableTable(TableName.valueOf("students"));

        admin.truncateTable(TableName.valueOf("students"), false);






        admin.close();
        conn.close();

    }
}
