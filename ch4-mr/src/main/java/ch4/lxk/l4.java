package ch4.lxk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class l4 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(l4.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/students_100w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/l4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public int[] scoreRank = new int[5];

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            Integer score = Integer.parseInt(toks[7]);
            if (score < 60) {
                scoreRank[0]++;
            } else if (score < 70) {
                scoreRank[1]++;
            } else if (score < 80) {
                scoreRank[2]++;
            } else if (score < 90) {
                scoreRank[3]++;
            } else {
                scoreRank[4]++;
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("<60"), new Text(scoreRank[0] + ""));
            context.write(new Text("60-70"), new Text(scoreRank[1] + ""));
            context.write(new Text("70-80"), new Text(scoreRank[2] + ""));
            context.write(new Text("80-90"), new Text(scoreRank[3] + ""));
            context.write(new Text("90-100"), new Text(scoreRank[4] + ""));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(count + ""));
        }

    }
}
