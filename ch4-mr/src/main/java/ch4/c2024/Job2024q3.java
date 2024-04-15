package ch4.c2024;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 最高最低分
 */
public class Job2024q3 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int maxVal = Integer.MIN_VALUE;
        int minVal = Integer.MAX_VALUE;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                int score = Integer.parseInt(toks[7]);
                if (score > maxVal) {
                    maxVal = score;
                }
                if (score < minVal) {
                    minVal = score;
                }
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("1"), new Text(maxVal + ""));
            context.write(new Text("1"), new Text(minVal + ""));
        }
    }

    static class MyReduce extends Reducer<Text, Text, Text, Text> {
        int maxVal = Integer.MIN_VALUE;
        int minVal = Integer.MAX_VALUE;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                int score = Integer.parseInt(val.toString());
                if (score > maxVal) {
                    maxVal = score;
                }
                if (score < minVal) {
                    minVal = score;
                }
            }
            context.write(new Text("最大值"), new Text(maxVal + ""));
            context.write(new Text("最小值"), new Text(minVal + ""));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2024q3.class.getSimpleName());
        job.setJarByClass(Job2024q3.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + Job2024q3.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
