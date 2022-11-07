package ch4.s202202;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
/**
 * 统计入边
 */
public class ch4c3PageRankV1 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "r1");
        job.setJarByClass(ch4c3PageRankV1.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path("E:\\data\\knowledge_graph\\freebase\\Release\\1_train.txt"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch403v1r1"));
        if (job.waitForCompletion(true)) {
            job = Job.getInstance(conf, "r2");
            job.setJarByClass(ch4c3PageRankV1.class);
            job.setMapperClass(JobMapper2.class);
            job.setReducerClass(JobReducer2.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/ch403v1r1"));
            FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch403v1r2"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                context.write(new Text(toks[2]), new Text("1"));
            }
        }
    }
    private static class JobReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count++;
            }
            context.write(key, new Text(count + ""));
        }
    }
    private static class JobMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                context.write(new IntWritable(Integer.parseInt(toks[1])), new Text(toks[0]));
            }
        }
    }
    private static class JobReducer2 extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(
                IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(new Text(key.toString()), value);
            }
        }
    }
}
