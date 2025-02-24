package exp2;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

/**
 * 将数据处理后导入HBase
 */
public class Exp201LoadDataIntoHBase {
    private static final byte[] _table_name = Bytes.toBytes("exp201");
    private static final byte[] _family = Bytes.toBytes("data");

    public static boolean checkAndCreateTable() {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(_table_name);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(_family).build();
                builder.setColumnFamily(familyDescriptor);
                TableDescriptor tableDescriptor = builder.build();
                admin.createTable(tableDescriptor);
            }
            conn.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        // 首先检查待写入的表是否存在，不存在的话新建
        if (checkAndCreateTable()) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "myjob");
            job.setJarByClass(Exp201LoadDataIntoHBase.class);
            job.setMapperClass(MyMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(MyReducer.class);
//            String localProjectPath = new File("E:\\data\\wikidata\\20220507\\").getAbsolutePath();
//            FileInputFormat.addInputPath(job, new Path(localProjectPath + "/data/zhwiki-latest-pages-articles-small.xml"));
//        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/exp201"));
            FileInputFormat.addInputPath(job, new Path("/zhwiki"));
//            FileInputFormat.addInputPath(job, new Path(localProjectPath+"/zhwiki-latest-pages-articles.xml"));
            TableMapReduceUtil.initTableReducerJob(new String(_table_name), MyReducer.class, job);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    /**
     * 我们在Mapper中将整个xml解析为json
     */
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        StringBuilder sb=new StringBuilder();
        SAXReader reader = new SAXReader();

        public void map(LongWritable key, Text value, Context context) {

            if (value.toString().contains("<page>")) {
                sb = new StringBuilder();
                sb.append(value);
            } else if (value.toString().contains("</page>")) {
                try {// 使用dom4j来解析xml，将xml解析为json
                    sb.append(value);
                    JSONObject jobj = new JSONObject();
                    Document doc = reader.read(new StringReader(sb.toString()));
                    Element root = doc.getRootElement();
                    Iterator i1 = root.elementIterator();
                    if (i1.hasNext()) {
                        while (i1.hasNext()) {
                            Element l1 = (Element) i1.next();
                            String n1 = l1.getName();
                            Iterator i2 = l1.elementIterator();
                            if (i2.hasNext()) {
                                while (i2.hasNext()) {
                                    Element l2 = (Element) i2.next();
                                    String n2 = l2.getName();
                                    Iterator i3 = l2.elementIterator();
                                    if (i3.hasNext()) {
                                        while (i3.hasNext()) {
                                            Element l3 = (Element) i3.next();
                                            String n3 = l3.getName();
                                            String valueText = l3.getText();
                                            if (valueText != null && valueText.trim().length() > 0) {
                                                jobj.put(n1 + "_" + n2 + "_" + n3, l3.getText());
                                            }
                                        }
                                    } else {
                                        String valueText = l2.getText();
                                        if (valueText != null && valueText.trim().length() > 0) {
                                            jobj.put(n1 + "_" + n2, l2.getText());
                                        }
                                    }
                                }
                            } else {
                                String valueText = l1.getText();
                                if (valueText != null && valueText.trim().length() > 0) {
                                    jobj.put(n1, l1.getText());
                                }
                            }
                        }
                    }
                    if (jobj.getString("id") != null) {
                        context.write(new Text(jobj.getString("id")), new Text(jobj.toJSONString()));
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                }
            } else {
                sb.append(value);
            }
        }
    }

    static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        Random rand = new Random();
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            String text = values.iterator().next().toString();
            JSONObject jobj = JSONObject.parseObject(text);
            Put put = new Put(key.getBytes());
            for (Map.Entry<String, Object> entry : jobj.entrySet()) {
                put.addColumn(_family,
                        Bytes.toBytes(entry.getKey()),
                        Bytes.toBytes(entry.getValue().toString()));
            }
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
        }
    }
}





























