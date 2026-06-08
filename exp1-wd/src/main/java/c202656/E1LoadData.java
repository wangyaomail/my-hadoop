package c202656;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class E1LoadData {
    public static void main(String[] args) throws Exception {
        String hadoopLocalHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopLocalHome);
        System.load(hadoopLocalHome + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        Table table = conn.getTable(TableName.valueOf("qa"));
        BufferedReader br = new BufferedReader(new FileReader("C:\\share\\data\\webtext2019zh\\web_text_zh_test.json"));
        List<String[]> toksList = new ArrayList<>();
        String line = null;
        List<Put> putList = new ArrayList<Put>();
        while ((line = br.readLine()) != null) {
            JSONObject obj = JSONObject.parseObject(line);
            String qid = obj.getString("qid");
            String title = obj.getString("title");
            String desc = obj.getString("desc");
            String topic = obj.getString("topic");
            String content = obj.getString("content");
            String star = obj.getString("star");
            String answer_id = obj.getString("answer_id");
            String answerer_tags = obj.getString("answerer_tags");
            Put put = new Put(Bytes.toBytes(qid+"-"+answer_id));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qid"),1l, Bytes.toBytes(qid));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"),1l, Bytes.toBytes(title));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("desc"),1l, Bytes.toBytes(desc));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("topic"),1l, Bytes.toBytes(topic));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"),1l, Bytes.toBytes(content));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("star"),1l, Bytes.toBytes(star));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answer_id"),1l, Bytes.toBytes(answer_id));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answerer_tags"),1l, Bytes.toBytes(answerer_tags));
            putList.add(put);

        }
        table.put(putList);

        admin.close();
        conn.close();
    }
}
