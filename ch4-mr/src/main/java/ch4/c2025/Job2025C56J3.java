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

/**
 * 最高最低分
 */
public class Job2025C56J3 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int maxValue = -1, minValue = 101;
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==8){
                int score = Integer.parseInt(toks[7]);
                if(score>maxValue){
                    maxValue = score;
                }
                if(score<minValue){
                    minValue = score;
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("1"),new Text(maxValue+""));
            context.write(new Text("1"),new Text(minValue+""));

        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        int maxValue = -1, minValue = 101;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                int score = Integer.parseInt(val.toString());
                if(score>maxValue){
                    maxValue = score;
                }
                if(score<minValue){
                    minValue = score;
                }
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("max"),new Text(maxValue+""));
            context.write(new Text("min"),new Text(minValue+""));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2025C56J3.class.getSimpleName());
        job.setJarByClass(Job2025C56J3.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job2025C56J3.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
