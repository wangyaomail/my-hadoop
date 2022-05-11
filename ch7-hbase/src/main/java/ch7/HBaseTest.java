package ch7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Map;

public class HBaseTest {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
//        for (Map.Entry<String, String> entry : conf) {
//            System.out.println("[" + entry.getKey() + "]:" + entry.getValue());
//        }
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        System.out.println(admin.getRegionServers());
        System.out.println(admin.getClusterMetrics());

        conn.close();

    }
}
