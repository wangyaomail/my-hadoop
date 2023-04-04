package ch4.c12;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job4MeanScore {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(Job4MeanScore.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(
                "C:\\nos\\my-hadoop\\data\\students_100w.data"
        ));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\job4"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int sum = 0, count = 0;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                sum += Integer.parseInt(toks[7]);
                count++;
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("1"), new Text(sum + "," + count));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int sum = 0, count = 0;
            int calCount =0;
            for (Text val : values) {
                String[] toks = val.toString().split(",");
                sum += Integer.parseInt(toks[0]);
                count += Integer.parseInt(toks[1]);
                calCount++;
            }
            context.write(new Text("均值"), new Text(((float) sum / count) + ""));
            context.write(new Text("计算次数"), new Text(calCount + ""));
        }
    }
}
