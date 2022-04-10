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
import java.util.*;

public class l2 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(l2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/students_100w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/l2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public HashMap<String, Integer> map = new HashMap<>();

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            String date = toks[4].substring(8);
            Integer count = map.get(date);
            if (count != null) {
                map.put(date, count + 1);
            } else {
                map.put(date, 1);
            }
        }
        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                context.write(new Text("1"), new Text(entry.getKey() + "\t" + entry.getValue() + ""));
            }
        }
    }
    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<>();
            for (Text value : values) {
                String[] toks = value.toString().split("\t");
                Integer count = map.get(toks[0]);
                if (count != null) {
                    map.put(toks[0], count + Integer.parseInt(toks[1]));
                } else {
                    map.put(toks[0],  Integer.parseInt(toks[1]));
                }
            }
            List<Map.Entry<String, Integer>> sortlist = new ArrayList<>(map.entrySet());
            sortlist.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            context.write(new Text("第三多"), new Text(sortlist.get(2) + ""));
            context.write(new Text("整个map"), new Text(sortlist.toString()));
        }
    }
}
