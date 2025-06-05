package c202556;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job2JsonParser {
    static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
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
            context.write(new Text(sb.toString()), NullWritable.get());

        }
    }

//    static class MyReducer extends Reducer<Text, Text, Text, Text> {
//        @Override
//        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            for (Text val : values) {
//                //
//            }
//        }
//    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2JsonParser.class.getSimpleName());
        job.setJarByClass(Job2JsonParser.class);
        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);
//        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\share\\data\\webtext2019zh\\web_text_zh_test.json"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job2JsonParser.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
