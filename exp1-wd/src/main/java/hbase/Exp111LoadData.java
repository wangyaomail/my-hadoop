package hbase;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import util.Sbc2Dbc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 这部分以HBase为主进行实验
 * 读取本地文件写入HBase
 */
public class Exp111LoadData {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        String fileName = "E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_test.json";
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        Table table = conn.getTable(TableName.valueOf("qa"));
        String line = null;
        List<Put> putList = new ArrayList<>();
        while ((line = br.readLine()) != null) {
            JSONObject json = JSONObject.parseObject(line.trim());
            String qid = cleanInputString(json.getString("qid"));
            String star = cleanInputString(json.getString("star"));
            String answer_id = cleanInputString(json.getString("answer_id"));
            String title = cleanInputString(json.getString("title"));
            String desc = cleanInputString(json.getString("desc"));
            String topic = cleanInputString(cleanInputString("topic"));
            String content = cleanInputString(json.getString("content"));
            String answerer_tags = cleanInputString(json.getString("answerer_tags"));
            if (StringUtils.isNotEmpty(qid) && StringUtils.isNotEmpty(answer_id)) {
                Put put = new Put(Bytes.toBytes(qid + "_" + answer_id));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qid"), Bytes.toBytes(qid));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"), Bytes.toBytes(title));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("star"), Bytes.toBytes(star));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("topic"), Bytes.toBytes(topic));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"), Bytes.toBytes(content));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answer_id"), Bytes.toBytes(answer_id));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answerer_tags"), Bytes.toBytes(answerer_tags));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("desc"), Bytes.toBytes(desc));
                putList.add(put);
            }
        }
        table.put(putList);
        conn.close();

    }

    public static String cleanInputString(String input) {
        if (input != null && input.length() > 0) {            // 去掉空格、换行、回车
            input = input.replaceAll("[\t\n ]+", "");            // 标点符号转全角，避免和程序中的字符冲突
            input = Sbc2Dbc.ToSBCWithoutLetterNumSpace(input);
        }
        return input;
    }

}
