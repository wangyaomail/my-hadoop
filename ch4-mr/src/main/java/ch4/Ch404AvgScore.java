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

/**
 * 去重姓氏
 */
public class Ch404AvgScore {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(Ch404AvgScore.class);
        job.setMapperClass(MyMapper.class);
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

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private int stuCount = 0, totalScore = 0;

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(",");
            if (toks.length == 8) {
                stuCount++;
                totalScore += Integer.parseInt(toks[7]);
            }
        }

        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text("score"), new Text(stuCount + "," + totalScore));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int stuCount = 0, totalScore = 0;
            for (Text value : values) {
                String[] toks = value.toString().split(",");
                stuCount += Integer.parseInt(toks[0]);
                totalScore += Integer.parseInt(toks[1]);
            }
            double avgScore = (double) totalScore / stuCount;
            context.write(key, new Text(avgScore + ""));
        }
    }
}
