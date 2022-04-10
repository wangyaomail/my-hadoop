package ch4.lxk;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class l1 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(l1.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/students_100w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/l1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public int boycount = 0;
        public int girlcount = 0;

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks[3].equals("男")) {
                boycount++;
            } else {
                girlcount++;
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int diff = boycount - girlcount;
            context.write(new Text("1"), new Text(diff + ""));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            context.write(new Text("男生比女生多"), new Text(count + ""));
        }
    }
}
