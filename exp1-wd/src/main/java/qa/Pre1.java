package qa;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Pre1 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "p1");
        job.setJarByClass(Pre1.class);
        job.setMapperClass(P1Mapper.class);
        job.setReducerClass(P1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,
                new Path("C:\\share\\data\\webtext2019zh\\web_text_zh_testa.json"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\p1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class P1Mapper extends Mapper<LongWritable, Text, Text, Text> {
        int count = 0;
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (count++ < 3) {
                context.write(new Text("1"), value);
            }
        }
    }

    private static class P1Reducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                if (count++ < 3) {
                    context.write(value, new Text(""));
                }
            }
        }
    }
}
