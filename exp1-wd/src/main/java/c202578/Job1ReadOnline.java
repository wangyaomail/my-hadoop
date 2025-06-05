package c202578;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job1ReadOnline {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int count = 0;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            if(count++ <3){
                context.write(value, new Text(""));
            }
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
        Job job = Job.getInstance(conf, Job1ReadOnline.class.getSimpleName());
        job.setJarByClass(Job1ReadOnline.class);
        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);
//        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/exp101"));
        FileOutputFormat.setOutputPath(job, new Path("/exp101_out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
