package c202312;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class HBaseLoadStudents {
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        String line = null;
        ArrayList<Put> puts = new ArrayList<>();
        while ((line = br.readLine())!=null){
            String[] toks = line.split("\t");
            if(toks.length==8){
                Put put = new Put(Bytes.toBytes(toks[2]));
                long ts = 1685116800000l;
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("name"),ts, Bytes.toBytes(toks[0]));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("clazz"),ts, Bytes.toBytes(toks[1]));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("gender"),ts, Bytes.toBytes(toks[3]));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("birthday"),ts, Bytes.toBytes(toks[4]));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("phone"), ts,Bytes.toBytes(toks[5]));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("loc"), ts,toks[6].getBytes("utf8"));
                put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("score"), ts,Bytes.toBytes(toks[7]));
                puts.add(put);
            }
        }
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(TableName.valueOf("students"));
        table.put(puts);
        table.close();
        admin.close();
        conn.close();
    }
}
