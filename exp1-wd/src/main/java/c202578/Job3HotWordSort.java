package c202578;
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

public class Job3HotWordSort {
    static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==2){
                if(toks[0].length()>1){
                    context.write(new IntWritable(-1*Integer.parseInt(toks[1])), new Text(toks[0]));
                }
            }
        }
    }

    static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, new Text((-1*key.get())+""));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job3HotWordSort.class.getSimpleName());
        job.setJarByClass(Job3HotWordSort.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/hotword"));
        FileOutputFormat.setOutputPath(job, new Path("/hotwordsort2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
