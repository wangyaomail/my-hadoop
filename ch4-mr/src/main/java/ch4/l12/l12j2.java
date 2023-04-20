package ch4.l12;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 出生在哪一月的人数最第三多？
 */
public class l12j2 {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(l12j2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,
                new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\lx12j21"));
        if (job.waitForCompletion(true)) {
            job = Job.getInstance(conf, "myjob");
            job.setJarByClass(l12j2.class);
            job.setMapperClass(MyMapper2.class);
            job.setReducerClass(MyReducer2.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job,
                    new Path("C:\\nos\\my-hadoop\\output\\lx12j21"));
            FileOutputFormat.setOutputPath(job,
                    new Path("C:\\nos\\my-hadoop\\output\\lx12j22"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                String month = toks[4].substring(5, 7);
                context.write(new Text(month), new Text(""));
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text t : values) {
                count++;
            }
            context.write(key, new Text(count + ""));
        }
    }

    private static class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                int score = Integer.parseInt(toks[1]);
                context.write(new IntWritable(score), new Text(toks[0]));
            }
        }
    }

    private static class MyReducer2 extends Reducer<IntWritable, Text, Text, Text> {
        List<String> months = new ArrayList<>();
        List<String> counts = new ArrayList<>();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                months.add(t.toString());
                counts.add(key.toString());
            }
        }
        @Override
        protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(months.toString()), new Text(""));
            context.write(new Text(counts.toString()), new Text(""));
            context.write(new Text(months.get(months.size()-1)), new Text(""));

            context.write(new Text(months.get(months.size()-2)), new Text(""));
            context.write(new Text(months.get(months.size()-3)), new Text(""));
        }
    }
}
