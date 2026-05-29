package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HBase1LoadData {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("students"));

        BufferedReader br = new BufferedReader(new FileReader("data/students_10w.data"));
        String line;
        long start = System.currentTimeMillis();
        long count = 0;
        List<Put> puts = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            count++;
            String[] toks = line.split("\t");
            if(toks.length == 8) {
                Put put = new Put(Bytes.toBytes(toks[2])); // 使用模拟的学生id作为key
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(toks[0]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"), Bytes.toBytes(toks[1]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sid"), Bytes.toBytes(toks[2]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes(toks[3]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes(toks[4]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"), Bytes.toBytes(toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"), Bytes.toBytes(toks[6]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes(toks[7]));
                puts.add(put);
            }
        }
        table.put(puts);
        long s1 = System.currentTimeMillis();
        System.out.println(s1 - start);


        conn.close();

    }
}
