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

import java.io.File;
import java.io.IOException;

public class Ch402HighScoreSlow {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(Ch402HighScoreSlow.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        File outputDir = new File(localProjectPath + "/test/ch4output");
        for (File f : outputDir.listFiles()) {
            f.delete();
        }
        outputDir.delete();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/data/students.csv"));
        FileOutputFormat.setOutputPath(job, new Path(outputDir.getAbsolutePath()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(",");
            if (toks.length == 8) {
                context.write(new Text("score"), new IntWritable(Integer.parseInt(toks[7])));
            }
        }
    }

    private static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE, minScore = Integer.MAX_VALUE;
            for (IntWritable value : values) {
                int num = value.get();
                if (num > maxScore) {
                    maxScore = num;
                }
                if (num < minScore) {
                    minScore = num;
                }
            }
            context.write(new Text("max:" + maxScore + " min:" + minScore), new Text(""));
        }
    }
}
