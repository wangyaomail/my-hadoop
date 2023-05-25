package c202334;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

/**
 * 最高最低分
 */
public class HBaseClassJob3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        String maxName="", minName="";
        int maxScore = -Integer.MAX_VALUE, minScore = Integer.MAX_VALUE;
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            String name = Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
            int score = Integer.parseInt(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("score"))));
            if (score > maxScore) {
                maxName = name;
                maxScore = score;
            }
            if (score < minScore) {
                minName = name;
                minScore = score;
            }
        }
        admin.close();
        conn.close();
        System.out.println(maxName + maxScore);
        System.out.println(minName + minScore);
    }
}
