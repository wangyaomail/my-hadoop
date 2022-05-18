package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CH710ImportStudensData extends CH701HBaseBase {
    @Override
    public void run() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("data/students_10w.data"));
        String line = null;
        Table table = conn.getTable(TableName.valueOf("students"));
        List<Put> putList = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            String[] toks = line.trim().split("\t");
            if (toks.length == 8) {
                Put put = new Put(Bytes.toBytes(toks[2]));
                //name, clazz, sid, gender, birthday, phone, loc, score
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(toks[0]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"), Bytes.toBytes(toks[1]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sid"), Bytes.toBytes(toks[2]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes(toks[3]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes(toks[4]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"), Bytes.toBytes(toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"), Bytes.toBytes(toks[6]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes(toks[7]));
                putList.add(put);
            }
        }
        table.put(putList);
        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH710ImportStudensData().run();
    }
}
