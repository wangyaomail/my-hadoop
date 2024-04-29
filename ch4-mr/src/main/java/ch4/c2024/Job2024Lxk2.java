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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 出生在哪一月的人数第5多？
 */
public class Job2024Lxk2 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> wordMap = new HashMap<>();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                String month = toks[4].substring(5, 7);
                Integer count = wordMap.get(month);
                if (count == null) {
                    wordMap.put(month, 1);
                } else {
                    wordMap.put(month, ++count);
                }
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            context.write(new Text("1"), new Text(sb.substring(0, sb.length() - 1)));
        }
    }

    static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> wordMap = new HashMap<>();
            for (Text val : values) {
                for (String valSplit : val.toString().split(",")) {
                    String[] toks = valSplit.split(":");
                    Integer count = wordMap.get(toks[0]);
                    if (count == null) {
                        wordMap.put(toks[0], Integer.parseInt(toks[1]));
                    } else {
                        wordMap.put(toks[0], Integer.parseInt(toks[1]) + count);
                    }
                }
            }
            List<Map.Entry<String, Integer>> wordList = wordMap.entrySet().stream().sorted(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return -o1.getValue() + o2.getValue();
                }
            }).collect(Collectors.toList());
            StringBuffer sb = new StringBuffer();
            for (Map.Entry<String, Integer> entry : wordList) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            context.write(new Text("第五个"), new Text(wordList.get(4).toString()));
            context.write(new Text("全部的"), new Text(sb.substring(0, sb.length() - 1)));

        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2024Lxk2.class.getSimpleName());
        job.setJarByClass(Job2024Lxk2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + Job2024Lxk2.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
