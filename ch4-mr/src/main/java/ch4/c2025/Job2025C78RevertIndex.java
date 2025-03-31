package ch4.c2025;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;

public class Job2025C78RevertIndex {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(" ");
            HashSet<String> wordSet = new HashSet<>();
            for (String word : toks) {
                wordSet.add(word);
            }
            for (String word : wordSet) {
                context.write(new Text(word), new Text(key.toString()));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(val).append(",");
            }
            context.write(key, new Text(sb.substring(0, sb.length() - 1)));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2025C78RevertIndex.class.getSimpleName());
        job.setJarByClass(Job2025C78RevertIndex.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\the_old_man_and_sea.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job2025C78RevertIndex.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
