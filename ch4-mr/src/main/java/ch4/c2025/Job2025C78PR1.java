package ch4.c2025;
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

public class Job2025C78PR1 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==3){
                context.write(new Text(toks[2]), new Text(""));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
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
            if(toks.length==2){
                context.write(new IntWritable(Integer.parseInt(toks[1])), new Text(toks[0]));
            }
        }
    }

    static class MyReducer2 extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(new Text(val.toString()), new Text(key.toString()));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2025C78PR1.class.getSimpleName()+"1");
        job.setJarByClass(Job2025C78PR1.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\share\\data\\FB15K-237.2\\Release\\train.txt"));
        String output1 = "C:\\nos\\my-hadoop\\output\\"+ Job2025C78PR1.class.getSimpleName()+".mid";
        String output2 = "C:\\nos\\my-hadoop\\output\\"+ Job2025C78PR1.class.getSimpleName()+".final";
        FileOutputFormat.setOutputPath(job, new Path(output1));
        if(job.waitForCompletion(true)){
            job = Job.getInstance(conf, Job2025C78PR1.class.getSimpleName()+"2");
            job.setJarByClass(Job2025C78PR1.class);
            job.setMapperClass(MyMapper2.class);
            job.setReducerClass(MyReducer2.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(output1));
            FileOutputFormat.setOutputPath(job, new Path(output2));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }
}
