import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class Exp103PreprocessC {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(Exp103PreprocessC.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path("/exp100"));

        TableMapReduceUtil.initTableReducerJob("exp103", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            JSONObject json = JSONObject.parseObject(value.toString());
            Integer qid = json.getInteger("qid");
            Integer star = json.getInteger("star");
            Integer answer_id = json.getInteger("answer_id");
            String title = cleanInputString(json.getString("title"));
            String desc = cleanInputString(json.getString("desc"));
            String topic = json.getString("topic");
            String content = cleanInputString(json.getString("content"));
            String answerer_tags = cleanInputString(json.getString("answerer_tags"));
            desc = desc==null || desc.length()==0 ? "null" :desc;
            answerer_tags = answerer_tags==null  || answerer_tags.length()==0 ?  "null" :answerer_tags;
            context.write(new Text(qid + "\t"
                    + title + "\t"
                    + star + "\t"
                    + topic + "\t"
                    + content + "\t"
                    + answer_id + "\t"
                    + answerer_tags + "\t"
                    + desc
            ), NullWritable.get());
        }

        public String cleanInputString(String input) {
            if (input != null && input.length() > 0) {            // 去掉空格、换行、回车
                input = input.replaceAll("[\t\n ]+", "");            // 标点符号转全角，避免和程序中的字符冲突
                input = Sbc2Dbc.ToSBCWithoutLetterNumSpace(input);
            }
            return input;
        }
    }

    private static class MyReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] toks = key.toString().split("\t");
            if (toks.length == 8) {
                Put put = new Put(Bytes.toBytes(toks[0] + "_" + toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qid"), Bytes.toBytes(toks[0]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"), Bytes.toBytes(toks[1]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("star"), Bytes.toBytes(toks[2]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("topic"), Bytes.toBytes(toks[3]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"), Bytes.toBytes(toks[4]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answer_id"), Bytes.toBytes(toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answerer_tags"), Bytes.toBytes(toks[6]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("desc"), Bytes.toBytes(toks[7]));
                context.write(new ImmutableBytesWritable(), put);
            } else {
                Put put = new Put(Bytes.toBytes(toks[0] + "_" + toks[5]));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("tokslen"), Bytes.toBytes(toks.length+""));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("xxx"), Bytes.toBytes(key.toString()));
                context.write(new ImmutableBytesWritable(), put);
            }
        }
    }


}
