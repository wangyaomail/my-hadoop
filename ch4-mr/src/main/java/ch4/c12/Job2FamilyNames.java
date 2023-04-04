package ch4.c12;
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
import java.util.HashSet;
import java.util.Set;

public class Job2FamilyNames {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(Job2FamilyNames.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(
                "C:\\nos\\my-hadoop\\data\\students_100w.data"
        ));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\job2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public Set<String> names= new HashSet<>();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
//            names.add(toks[0].split("")[0]);
            context.write(new Text(toks[0].substring(0,1)), new Text(""));
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for(String name: names){
//                context.write(new Text(name), new Text(""));
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }
}
