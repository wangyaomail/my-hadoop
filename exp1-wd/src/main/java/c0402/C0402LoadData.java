package c0402;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;

public class C0402LoadData {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("qa"));
        BufferedReader br = new BufferedReader(new FileReader("C:\\share\\data\\webtext2019zh\\web_text_zh_testa.json"));
        String line = null;
        ArrayList<Put> putList = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            JSONObject jobj = JSONObject.parseObject(line);
            String qid = jobj.getString("qid");
            String star = jobj.getString("star");
            String answer_id = jobj.getString("answer_id");
            String title = jobj.getString("title");
            String desc = jobj.getString("desc");
            String topic = jobj.getString("topic");
            String content = jobj.getString("content");
            String answerer_tags = jobj.getString("answerer_tags");
            Put put = new Put(Bytes.toBytes(qid + "-" + answer_id));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("qid"),
                    Bytes.toBytes(qid));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("star"),
                    Bytes.toBytes(star));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("answer_id"),
                    Bytes.toBytes(answer_id));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("title"),
                    Bytes.toBytes(title));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("desc"),
                    Bytes.toBytes(desc));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("topic"),
                    Bytes.toBytes(topic));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("content"),
                    Bytes.toBytes(content));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("answerer_tags"),
                    Bytes.toBytes(answerer_tags));
            putList.add(put);
        }
        table.put(putList);
        conn.close();
    }
}
