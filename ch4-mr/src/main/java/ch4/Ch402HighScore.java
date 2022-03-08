package ch4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Ch402HighScore {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(Ch402HighScore.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        File OutputDir = new File(localProjectPath + "/test/ch4output");
        for (File f : OutputDir.listFiles()) {
            f.delete();
        }
        OutputDir.delete();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/data/students.csv"));
        FileOutputFormat.setOutputPath(job, new Path(OutputDir.getAbsolutePath()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private int maxScore = 0, minScore = 100;

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(",");
            if (toks.length == 8) {
                int num = Integer.parseInt(toks[7]);
                if (num > maxScore) {
                    maxScore = num;
                }
                if (num < minScore) {
                    minScore = num;
                }
            }
        }

        protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text("score"), new IntWritable(maxScore));
            context.write(new Text("score"), new IntWritable(minScore));
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
