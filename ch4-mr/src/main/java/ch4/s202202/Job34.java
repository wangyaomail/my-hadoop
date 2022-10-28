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
 * 最值：正确示范2
 */
public class Job34 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(Job34.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path(localProjectPath + "/data/students"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/job3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class JobMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        int maxScore = 0;
        int minScore = 100;
        String maxName = "";
        String minName = "";
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                int score = Integer.parseInt(toks[7]);
                if (score >= maxScore) {
                    maxScore = score;
                    maxName += ","+toks[0];
                } else if (score <= minScore) {
                    minScore = score;
                    minName += ","+toks[0];
                }
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(maxScore), new Text(maxName));
            context.write(new IntWritable(minScore), new Text(minName));
        }
    }
    private static class JobReducer extends Reducer<IntWritable, Text, Text, Text> {
        boolean isFirst = true;
        String lastScore = null;
        String lastName = null;
        public void reduce(
                IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder nameStr = new StringBuilder();
            for (Text value : values) {
                nameStr.append(value.toString()).append(",");
            }
            if (isFirst) {
                context.write(new Text(key.toString()), new Text(nameStr.toString()));
                isFirst = false;
            } else {
                lastScore = key.toString();
                lastName = nameStr.toString();
            }
        }
        @Override
        protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(lastScore), new Text(lastName));
        }
    }
}
