package c2023;
import ch7.CH701HBaseBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class DaoRu  extends CH701HBaseBase {
    TableName tableName = TableName.valueOf("students");

    @Override
    public void run() throws IOException {
        Table table = conn.getTable(tableName);
        ArrayList<Put> putList = new ArrayList<>();
        try {

            BufferedReader br = new BufferedReader(new FileReader("data/students_100w.data"));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] toks = line.split("\t");
                if ( toks.length!=8){
                    System.out.println("error！！！！");
                } else{
                    Put put = new Put(Bytes.toBytes(toks[2]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(toks[0]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"), Bytes.toBytes(toks[1]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("id"), Bytes.toBytes(toks[2]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes(toks[3]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes(toks[4]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"), Bytes.toBytes(toks[5]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("home"), Bytes.toBytes(toks[6]));
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes(toks[7]));
                    putList.add(put);
                    if(putList.size()>10000){
                        table.put(putList);
                        putList.clear();
                    }
                }
            }
            table.put(putList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        conn.close();

    }

    public static void main(String[] args) throws IOException {
        new DaoRu().run();
    }


}
