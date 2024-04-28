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
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Job2024S2FrontIndex {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            ArrayList<Character> valueTrans = new ArrayList<>();
            for (char c : value.toString().toCharArray()) {
                if ((c >= 'a' && c <= 'z') || c == ' ') {
                    valueTrans.add(c);
                } else if (c >= 'A' && c <= 'Z') {
                    char c2 = (char) (c - ('A' - 'a'));
                    valueTrans.add(c2);
                }
            }
            char[] valueChars = new char[valueTrans.size()];
            for (int i = 0; i < valueTrans.size(); i++) {
                valueChars[i] = valueTrans.get(i);
            }
            String valueString = new String(valueChars);
            String[] toks = valueString.trim().split(" ");
            HashMap<String, Integer> wordMap = new HashMap<>();
            for (String tok : toks) {
                Integer num = wordMap.get(tok);
                if (num == null) {
                    wordMap.put(tok, 1);
                } else {
                    wordMap.put(tok, ++num);
                }
            }
            List<Map.Entry<String, Integer>> wordList = wordMap.entrySet().stream().sorted(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return -o1.getValue() + o2.getValue();
                }
            }).collect(Collectors.toList());
            if (wordList.size() > 1) {
                StringBuffer sb = new StringBuffer();
                for (Map.Entry<String, Integer> entry : wordList) {
                    sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
                }
                context.write(new Text(key.toString()), new Text(sb.substring(0, sb.length() - 1)));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2024S2FrontIndex.class.getSimpleName());
        job.setJarByClass(Job2024S2FrontIndex.class);
        job.setMapperClass(MyMapper.class);
//        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\share\\data\\the_old_man_and_sea.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\" + Job2024S2FrontIndex.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
