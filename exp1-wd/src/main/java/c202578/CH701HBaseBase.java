package c202578;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class CH701HBaseBase {
    public static String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
    static {
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
    }
    public Connection conn = null;
    public Admin admin = null;
    public CH701HBaseBase() {
        try {
            Configuration conf = HBaseConfiguration.create();
            for (Iterator<Map.Entry<String, String>> it = conf.iterator(); it.hasNext(); ) {
                Map.Entry<String, String> entry = it.next();
                System.out.println("参数[" + entry.getKey() + "]=" + entry.getValue());
            }
            this.conn = ConnectionFactory.createConnection(conf);
            this.admin = conn.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    public void run() throws IOException {
        ClusterMetrics metrics = admin.getClusterMetrics();
        System.out.println(metrics);
    }
    public static void main(String[] args) throws IOException {
        new CH701HBaseBase().run();
    }
}
