package c202334;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class HBaseStudentsPut {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("students");
        Table table = conn.getTable(tableName);
        BufferedReader br = new BufferedReader(new FileReader("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        String line = null;
        List<Put> puts = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            String[] toks = line.trim().split("\t");
            if (toks.length == 8) {
                Put put = new Put(Bytes.toBytes(toks[2]));
                long ts = 1685030400000l;
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"),ts, Bytes.toBytes(toks[0]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"),ts, Bytes.toBytes(toks[1]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"),ts, Bytes.toBytes(toks[3]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"),ts, Bytes.toBytes(toks[4]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"),ts, Bytes.toBytes(toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"),ts, Bytes.toBytes(toks[6]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"),ts, Bytes.toBytes(toks[7]));
                puts.add(put);
            }
        }
        table.put(puts);
        admin.close();
        table.close();
        conn.close();
    }
}
