package ch4.s202202;
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
import java.util.*;
/**
 * 手机号中出现最多的数字？
 */
public class Lxk3 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(Lxk3.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path(localProjectPath + "/data/students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/lxk3"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        public int[] month = new int[10];
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                String[] num_str_s = toks[5].split("");
                for (String num_str : num_str_s) {
                    month[Integer.parseInt(num_str)]++;
                }
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (int m : month) {
                sb.append(m).append(",");
            }
            context.write(new Text("1"), new Text(sb.substring(0, sb.length() - 1)));
        }
    }
    private static class JobReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<Integer, Integer> month_count_all = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                month_count_all.put(i, 0);
            }
            for (Text v : values) {
                String[] month_counts = v.toString().split(",");
                for (int i = 0; i < 10; i++) {
                    int month_int = Integer.parseInt(month_counts[i]);
                    int old_val = month_count_all.get(i);
                    month_count_all.put(i, old_val + month_int);
                }
            }
            List<Map.Entry<Integer, Integer>> sortList = new ArrayList<>(month_count_all.entrySet());
            sortList.sort(new Comparator<Map.Entry<Integer, Integer>>() {
                @Override
                public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                    return -o1.getValue() + o2.getValue();
                }
            });
            context.write(new Text(sortList.get(0).toString()), new Text(""));
            context.write(new Text(sortList.toString()), new Text(""));
        }
    }
}
