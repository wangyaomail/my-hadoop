package ch4;

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

public class PageRankV1CalculateB {
    public static void main(String[] args) throws Exception {
        String hadoopHome = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopHome);
        System.load(hadoopHome + "\\bin\\hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(PageRankV1CalculateB.class);
        job.setMapperClass(WCMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\wangy\\Desktop\\my-hadoop-maven-template\\test\\out8"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\wangy\\Desktop\\my-hadoop-maven-template\\test\\out9"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class WCMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                context.write(new IntWritable(Integer.parseInt(toks[1])), new Text(toks[0]));
            }
        }

    }

    private static class WCReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text(key.get() + ""));
            }
        }
    }
}
