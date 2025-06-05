package c202556;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job2JsonParserFormat {
    static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            JSONObject jobj = JSONObject.parseObject(value.toString());
            StringBuilder sb = new StringBuilder();
            sb.append(jobj.getString("qid")).append("\t");
            sb.append(jobj.getString("title")).append("\t");
            sb.append(jobj.getString("desc")).append("\t");
            sb.append(jobj.getString("topic")).append("\t");
            sb.append(jobj.getString("star")).append("\t");
            sb.append(jobj.getString("content")).append("\t");
            sb.append(jobj.getString("answer_id")).append("\t");
            sb.append(jobj.getString("answerer_tags"));
            context.write(new IntWritable(jobj.getInteger("qid")), new Text(sb.toString()));
        }
    }

    static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(new Text(key.toString()),
                        new Text(val.toString()));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2JsonParserFormat.class.getSimpleName());
        job.setJarByClass(Job2JsonParserFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\share\\data\\webtext2019zh\\web_text_zh_test.json"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job2JsonParserFormat.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
