package ch4.c2024;
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
import java.util.Map;

/**
 * 已知四连号是吉祥号，哪个班吉祥号最多？
 */
public class Job2024Lxk6 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        String[] lianhao = {"1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999"};
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                for (String lh : lianhao) {
                    if (toks[5].contains(lh)) {
                        context.write(new Text("1"), new Text(toks[1]));
                        break;
                    }
                }
            }
        }
    }

    static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> wordMap = new HashMap<>();
            for (Text val : values) {
                Integer count = wordMap.get(val.toString());
                if (count == null) {
                    wordMap.put(val.toString(), 1);
                } else {
                    wordMap.put(val.toString(), ++count);
                }
            }
            String maxPro = "";
            int count = 0;
            for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                if (entry.getValue() > count) {
                    maxPro = entry.getKey();
                    count = entry.getValue();
                }
            }
            context.write(new Text("最多的是" + maxPro), new Text(count + ""));
            context.write(new Text("全部的"), new Text(wordMap.toString()));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2024Lxk6.class.getSimpleName());
        job.setJarByClass(Job2024Lxk6.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + Job2024Lxk6.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
