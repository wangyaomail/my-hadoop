package hadoophive;

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

public class Exp104HotAnswer {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job");
        job.setJarByClass(Exp104HotAnswer.class);
        job.setMapperClass(MyMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/exp102"));
        FileOutputFormat.setOutputPath(job, new Path("/exp104"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int maxStar = Integer.MIN_VALUE;
        String answerInfo = "";

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                int star = Integer.parseInt(toks[2]);
                if (star > maxStar) {
                    maxStar = star;
                    answerInfo = toks[0] + "\t" + toks[5] + "\t" + toks[1] + "\t" + toks[4];
                }
            }
        }

        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            context.write(new Text("score"), new Text(maxStar + "::" + answerInfo));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            String answerInfo = "";
            for (Text value : values) {
                String[] toks = value.toString().trim().split("::");
                int star = Integer.parseInt(toks[0]);
                if (star > maxScore) {
                    maxScore = star;
                    answerInfo = toks[1];
                }
//                context.write(new Text(value), new Text(""));
            }
            context.write(new Text(maxScore + ""), new Text(answerInfo));
        }
    }
}
