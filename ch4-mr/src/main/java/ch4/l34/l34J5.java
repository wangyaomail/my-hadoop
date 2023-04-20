package ch4.l34;
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
import java.util.LinkedList;
import java.util.List;

/**
 * 在2001年出生的数量最多的三个姓氏是什么？
 */
public class l34J5 {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(l34J5.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/data/students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/output/l34j51"));
        if (job.waitForCompletion(true)) {
            job = Job.getInstance(conf, "MyJob");
            job.setJarByClass(l34J5.class);
            job.setMapperClass(MyMapper1.class);
            job.setReducerClass(MyReducer1.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(localProjectPath + "/output/l34j51"));
            FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/output/l34j52"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                String name = toks[0];
                context.write(new Text(name.substring(0,1)), new Text(""));
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int score = 0;
            for (Text value : values) {
                score++;
            }
            context.write(key, new Text(score + ""));
        }
    }

    private static class MyMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                context.write(new IntWritable(Integer.parseInt(toks[1])),
                        new Text(toks[0]));
            }
        }
    }

    private static class MyReducer1 extends Reducer<IntWritable, Text, Text, Text> {
        List<String> names = new LinkedList<>();
        public void reduce(
                IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                names.add(value.toString());
            }
        }
        @Override
        protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(names.get(names.size()-1)), new Text(""));
            context.write(new Text(names.get(names.size()-2)), new Text(""));
            context.write(new Text(names.get(names.size()-3)), new Text(""));

        }
    }
}
