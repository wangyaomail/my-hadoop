package c202312;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashSet;

public class HBaseJob3 {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        ResultScanner rsc = table.getScanner(scan);
        HashSet<String> nameSet = new HashSet<>();
        int maxScore = -100,minScore=200;
        int avgScore=0,count=0;
        for (Result r : rsc) {
            int score = Integer.parseInt(Bytes.toString(r.getValue(Bytes.toBytes("data"), Bytes.toBytes("score"))));
            avgScore+=score;
            count++;
            if(score>maxScore){
                maxScore = score;
            }
            if(score<minScore){
                minScore=score;
            }
        }
        System.out.println("最高分"+maxScore);
        System.out.println("最低分"+minScore);
        System.out.println("平均分"+(avgScore/count));
        admin.close();
        conn.close();
    }
}
