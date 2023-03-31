package hadoophive;

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

public class Exp105HotQuestion {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        {
            Job job = Job.getInstance(conf, "job");
            job.setJarByClass(Exp105HotQuestion.class);
            job.setMapperClass(MyMapper1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(MyReducer1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/exp102"));
            FileOutputFormat.setOutputPath(job, new Path("/exp105a"));
            if (job.waitForCompletion(true)) {
            }
        }
        {
            Job job = Job.getInstance(conf, "job");
            job.setJarByClass(Exp105HotQuestion.class);
            job.setMapperClass(MyMapper2.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(MyReducer2.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/exp105a"));
            FileOutputFormat.setOutputPath(job, new Path("/exp105"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    private static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            context.write(new Text(toks[0]), new Text(toks[2] + "\t" + toks[1]));
        }

    }

    private static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int starAll = 0;
            String title = "";
            for (Text value : values) {
                String[] toks = value.toString().trim().split("\t");
                int star = Integer.parseInt(toks[0]);
                starAll += star;
                title = toks[1];
            }
            context.write(new Text(starAll + ""), new Text(title));
        }
    }

    private static class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                int star = Integer.parseInt(toks[0]);
                context.write(new IntWritable(star), value);
            }
        }

    }

    private static class MyReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
