package ch4.c2024;
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

/**
 * 迭代训练
 */
public class Job2024S2PR2b {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 4) {
                String[] outs = toks[3].split(",");
                double p1 = Double.parseDouble(toks[1]);
                double p2 = Double.parseDouble(toks[2]);
                double p3 = p1 * p2;
                for (String out : outs) {
                    context.write(new Text(out), new Text("a" + "\t" + p3));
                }
                context.write(new Text(toks[0]), new Text("b" + "\t" + toks[2] + "\t" + toks[3]));
            }
        }
    }

    static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            double stayP = 0.0;
            String jumpP = "";
            String jumpM = "";
            for (Text val : values) {
                String[] toks = val.toString().trim().split("\t");
                if (toks[0].equals("a")) {
                    stayP += Double.parseDouble(toks[1]);
                } else if (toks[0].equals("b")) {
                    jumpP = toks[1];
                    jumpM = toks[2];
                }
            }
            StringBuilder sb = new StringBuilder();
            sb.append(stayP).append("\t");
            sb.append(jumpP).append("\t");
            sb.append(jumpM);
            context.write(key, new Text(sb.toString()));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        String inputPath = "C:\\nos\\my-hadoop\\output\\Job2024S2PR2b";
        for (int i = 0; i < 10; i++) {
            Job job = Job.getInstance(conf, Job2024S2PR2b.class.getSimpleName());
            job.setJarByClass(Job2024S2PR2b.class);
            job.setMapperClass(MyMapper.class);
            job.setReducerClass(MyReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(inputPath + i));
            FileOutputFormat.setOutputPath(job, new Path(inputPath + (i + 1)));
            if (job.waitForCompletion(true)) {
                continue;
            }
        }
    }
}
