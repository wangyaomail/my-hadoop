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
 * 2002年出生的同学中最多的三大姓氏是什么？
 */
public class Lxk5 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(Lxk5.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path(localProjectPath + "/data/students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/lxk5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> fanames = new HashMap<>();
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                if (toks[4].startsWith("2002")) {
                    String faname = toks[0].split("")[0];
                    if (fanames.containsKey(faname)) {
                        fanames.put(faname, fanames.get(faname) + 1);
                    } else {
                        fanames.put(faname, 1);
                    }
                }
            }
        }
        // 张:3;李:5;王:10;
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : fanames.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
            }
            context.write(new Text("1"), new Text(sb.substring(0, sb.length() - 1)));
        }
    }
    private static class JobReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> fanames = new HashMap<>();
            for (Text v : values) {
                String[] ncs = v.toString().split(";");
                for (String nc : ncs) {
                    String[] nc_split = nc.split(":");
                    String faname = nc_split[0];
                    if (fanames.containsKey(faname)) {
                        fanames.put(faname, fanames.get(faname) + Integer.parseInt(nc_split[1]));
                    } else {
                        fanames.put(faname, Integer.parseInt(nc_split[1]));
                    }
                }
            }
            List<Map.Entry<String, Integer>> sortList = new ArrayList<>(fanames.entrySet());
            sortList.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return -o1.getValue() + o2.getValue();
                }
            });
            context.write(new Text(sortList.get(0).toString()), new Text(""));
            context.write(new Text(sortList.get(1).toString()), new Text(""));
            context.write(new Text(sortList.get(2).toString()), new Text(""));
            context.write(new Text(sortList.toString()), new Text(""));
        }
    }
}
