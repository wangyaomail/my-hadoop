package c202556;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Job2LoadToHBase extends CH701HBaseBase {
    @Override
    public void run() throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("output/Job2JsonParserFormat//part-r-00000"));
        String line = null;
        Table table = conn.getTable(TableName.valueOf("qa"));
        List<Put> putList = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            String[] toks = line.trim().split("\t");
            if (toks.length == 8) {
                Put put = new Put(Bytes.toBytes(toks[0]+"_"+toks[6]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qid"), Bytes.toBytes(toks[0]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"), Bytes.toBytes(toks[1]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("desc"), Bytes.toBytes(toks[2]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("topic"), Bytes.toBytes(toks[3]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("star"), Bytes.toBytes(toks[4]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"), Bytes.toBytes(toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answer_id"), Bytes.toBytes(toks[6]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answerer_tags"), Bytes.toBytes(toks[7]));

               putList.add(put);
            }
        }
        table.put(putList);
        conn.close();
    }
    public static void main(String[] args) throws IOException {
        new Job2LoadToHBase().run();
    }
}
