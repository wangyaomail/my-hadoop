package ch4.c2025.lxk78;
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

public class Job2025l8 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        double g1 = 0;
        int g2 = 0;
        double b1 = 0;
        int b2 = 0;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(",");
            if(toks.length==10){
                double htl = Double.parseDouble(toks[8]);
                if(toks[9].equals("是")){
                    g1 += htl;
                    g2 ++;
                } else {
                    b1 += htl;
                    b2 ++;
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text(g1+","+g2+","+b1+","+b2));
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            double g1 = 0;
            int g2 = 0;
            double b1 = 0;
            int b2 = 0;
            for (Text val : values) {
                String[] toks = val.toString().split(",");
                g1 += Double.parseDouble(toks[0]);
                g2 += Double.parseDouble(toks[1]);
                b1 += Double.parseDouble(toks[2]);
                b2 += Double.parseDouble(toks[3]);
            }
            double htlcz = g1/g2 - b1/b2;
            context.write(key, new Text(htlcz+""));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2025l8.class.getSimpleName());
        job.setJarByClass(Job2025l8.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\xigua3.0.csv"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job2025l8.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
