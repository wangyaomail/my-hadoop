package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HBase1LoadData {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        Table table1 = conn.getTable(TableName.valueOf("students"));
        Table table2 = conn.getTable(TableName.valueOf("students"));

        BufferedReader br = new BufferedReader(new FileReader("data/students_10w.data"));
        List<String[]> toksList = new ArrayList<>();
        String line = null;
        while ((line = br.readLine()) != null) {
            String[] toks = line.split("\t");
            if(toks.length == 8){
                toksList.add(toks);
            }
        }
        List<Put> putList = new ArrayList<Put>();
        long t1 = System.currentTimeMillis();
//        for(String[] toks:toksList){
//            Put put = new Put(Bytes.toBytes(toks[2])); // 使用模拟的学生id作为key
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(toks[0]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"), Bytes.toBytes(toks[1]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sid"), Bytes.toBytes(toks[2]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes(toks[3]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes(toks[4]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"), Bytes.toBytes(toks[5]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"), Bytes.toBytes(toks[6]));
//            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes(toks[7]));
//            table1.put(put);
//        }
        long t2 = System.currentTimeMillis();
        for(String[] toks:toksList){
            Put put = new Put(Bytes.toBytes(toks[2])); // 使用模拟的学生id作为key
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"),1l, Bytes.toBytes(toks[0]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"),1l, Bytes.toBytes(toks[1]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sid"),1l, Bytes.toBytes(toks[2]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"),1l, Bytes.toBytes(toks[3]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"),1l, Bytes.toBytes(toks[4]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"),1l, Bytes.toBytes(toks[5]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"),1l, Bytes.toBytes(toks[6]));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"),1l, Bytes.toBytes(toks[7]));
            putList.add(put);
        }
        table2.put(putList);
        long t3 = System.currentTimeMillis();
        System.out.println((t2-t1)+";"+(t3-t2));

        admin.close();
        conn.close();
    }
}
