package ch4.c12;
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
import java.util.HashSet;
import java.util.Set;

public class Job3HighScore {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(Job3HighScore.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(
                "C:\\nos\\my-hadoop\\data\\students_100w.data"
        ));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\job3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                context.write(new IntWritable(Integer.parseInt(toks[7])), new Text(""));
            }
        }
    }

    private static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
        int count = 0;
        int maxScore = 0;
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            if (count++ == 0) {
                context.write(new Text("最低分"), new Text(key.toString()));
            } else {
                maxScore = key.get();
            }
        }
        @Override
        protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("最高分"), new Text(maxScore + ""));
        }
    }
}
