package qa;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Pre2 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "p2");
        job.setJarByClass(Pre2.class);
        job.setMapperClass(P2Mapper.class);
//        job.setReducerClass(P2Reducer.class);
        FileInputFormat.addInputPath(job,
                new Path("/p2/web_text_zh_testa.json"));

//                new Path("C:\\share\\data\\webtext2019zh\\web_text_zh_testa.json"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\p1"));
        TableMapReduceUtil.initTableReducerJob(
                "qa",
                P2Reducer.class,
                job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class P2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(new Text("1"), value);
        }
    }

    private static class P2Reducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                JSONObject jobj = JSONObject.parseObject(value.toString());
//                qid、title、topic、star、content、answer_id、answerer_tags
                String qid = jobj.getString("qid");
                String title = jobj.getString("title");
                String topic = jobj.getString("topic");
                String star = jobj.getString("star");
                String content = jobj.getString("content");
                String answer_id = jobj.getString("answer_id");
                String answerer_tags = jobj.getString("answerer_tags");
                byte[] rowkey = Bytes.toBytes(qid + "-" + answer_id);
                Put put = new Put(rowkey);
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("qid"),
                        Bytes.toBytes(qid));
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("title"),
                        Bytes.toBytes(title));
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("topic"),
                        Bytes.toBytes(topic));
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("star"),
                        Bytes.toBytes(star));
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("content"),
                        Bytes.toBytes(content));
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("answer_id"),
                        Bytes.toBytes(answer_id));
                put.addColumn(
                        Bytes.toBytes("data"),
                        Bytes.toBytes("answerer_tags"),
                        Bytes.toBytes(answerer_tags));
                context.write(new ImmutableBytesWritable(rowkey), put);
            }
        }
    }
}
