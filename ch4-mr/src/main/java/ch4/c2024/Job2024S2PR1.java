package ch4.c2024;
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

import java.io.IOException;

public class Job2024S2PR1 {
    static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                context.write(new Text(toks[2]), new Text("1"));
            }
        }
    }

    static class MyReduce1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text val : values) {
                count++;
            }
            context.write(key, new Text(Integer.toString(count)));
        }
    }

    static class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                context.write(new IntWritable(-Integer.parseInt(toks[1])), new Text(toks[0]));
            }
        }
    }

    static class MyReduce2 extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, new Text((-1 * key.get()) + ""));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        String jobName1 = Job2024S2PR1.class.getSimpleName() + "a";
        String jobName2 = Job2024S2PR1.class.getSimpleName() + "b";
        Job job = Job.getInstance(conf, jobName1);
        job.setJarByClass(Job2024S2PR1.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReduce1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\share\\data\\FB15K-237.2\\Release\\train.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + jobName1));
        if (job.waitForCompletion(true)) {
            job = Job.getInstance(conf, Job2024S2PR1.class.getSimpleName() + "b");
            job.setJarByClass(Job2024S2PR1.class);
            job.setMapperClass(MyMapper2.class);
            job.setReducerClass(MyReduce2.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + jobName1));
            FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + jobName2));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
