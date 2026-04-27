package ch4.c2026.c56;
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
import java.util.HashMap;

public class Job20260427L3各班平均分2 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> scores =  new HashMap<>();
        HashMap<String, Integer> counts =  new HashMap<>();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==8){
                if(!scores.containsKey(toks[1])){
                    scores.put(toks[1],Integer.parseInt(toks[7]));
                    counts.put(toks[1],1);
                } else {
                    scores.put(toks[1],scores.get(toks[1])+Integer.parseInt(toks[7]));
                    counts.put(toks[1],counts.get(toks[1])+1);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for(String key : scores.keySet()){
                context.write(new Text(key), new Text(counts.get(key)+"\t"+scores.get(key)));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (Text val : values) {
                String[] count_score = val.toString().trim().split("\t");
                sum += Integer.parseInt(count_score[1]);
                count += Integer.parseInt(count_score[0]);
            }
            context.write(key, new Text(1.0*sum/count+""));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job20260427L3各班平均分2.class.getSimpleName());
        job.setJarByClass(Job20260427L3各班平均分2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job20260427L3各班平均分2.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
