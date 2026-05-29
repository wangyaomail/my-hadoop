package c202656;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;

public class HBaseFirst {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        Get get = new Get(Bytes.toBytes("1"));
        Table table  = conn.getTable(TableName.valueOf("students"));
        Result rs = table.get(get);
        byte[] nameByte = rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("name"));
        System.out.println(Bytes.toString(nameByte));


        admin.close();
        conn.close();
    }
}
