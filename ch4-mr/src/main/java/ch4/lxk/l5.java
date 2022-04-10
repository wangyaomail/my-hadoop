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
import java.util.HashMap;
import java.util.Map;

public class l5 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(l5.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/students_100w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/l5"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            String clazz = toks[1];
            String conv = "";
            if (toks[6].contains("省")) {
                conv = toks[6].split("省")[0] + "省";
            } else if (toks[6].contains("自治区")) {
                conv = toks[6].split("自治区")[0] + "自治区";
            } else if (toks[6].contains("市")) {
                conv = toks[6].split("市")[0] + "市";
            } else {
                conv = toks[6].substring(0, 3);
            }
            context.write(new Text(clazz), new Text(conv));
        }

    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String, Integer> map = new HashMap<>();
            for (Text value : values) {
                Integer count = map.get(value.toString());
                if (count != null) {
                    map.put(value.toString(), count + 1);
                } else {
                    map.put(value.toString(), 1);
                }
            }
            context.write(key, new Text(map.toString()));
            int total = 0, count = 0;
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                total += entry.getValue();
                count++;
            }
            double avg = (double) total / count;
            double junyun = 0;
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                junyun += Math.abs(entry.getValue() - avg);
            }
            context.write(key, new Text("均匀程度" + junyun));
        }

    }
}
