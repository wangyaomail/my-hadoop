import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
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

import java.io.IOException;

public class Exp106HotWord {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        {
            Job job = Job.getInstance(conf, "job");
            job.setJarByClass(Exp106HotWord.class);
            job.setMapperClass(MyMapper1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setReducerClass(MyReducer1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/exp102"));
            FileOutputFormat.setOutputPath(job, new Path("/exp106a"));
            if (job.waitForCompletion(true)) {
            }
        }
        {
            Job job = Job.getInstance(conf, "job");
            job.setJarByClass(Exp106HotWord.class);
            job.setMapperClass(MyMapper2.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(MyReducer2.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/exp106a"));
            FileOutputFormat.setOutputPath(job, new Path("/exp106"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    private static class MyMapper1 extends Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            Result result = ToAnalysis.parse(toks[4]);
            for(Term term :result.getTerms()){
                if(term.getName().length()>1) {
                    context.write(new Text(term.getName()), NullWritable.get());
                }
            }
        }

    }

    private static class MyReducer1 extends Reducer<Text, NullWritable, Text, Text> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (NullWritable value : values) {
                count++;
            }
            context.write(key, new Text(count+""));
        }
    }

    private static class MyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 2) {
                int count = Integer.parseInt(toks[1]);
                context.write(new IntWritable(count), new Text(toks[0]));
            }
        }

    }

    private static class MyReducer2 extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
}
