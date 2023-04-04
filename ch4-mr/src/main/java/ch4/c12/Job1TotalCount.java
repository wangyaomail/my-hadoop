package ch4.c12;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

public class Job1TotalCount {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(Job1TotalCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(
                "C:\\nos\\my-hadoop\\data\\students_100w.data"
        ));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\job1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int mancount = 0;
        int womancount = 0;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks[3].equals("男")) {
                mancount++;
            } else {
                womancount++;
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("男"), new Text(mancount + ""));
            context.write(new Text("女"), new Text(womancount + ""));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text val : values) {
                count += Integer.parseInt(val.toString());
            }
            context.write(key, new Text(count + ""));
        }
    }
}
