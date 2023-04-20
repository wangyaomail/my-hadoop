package ch4.l34;
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

/**
 * 已知五连号是吉祥号，哪个班没有吉祥号？
 */
public class l34J6 {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MyJob");
        job.setJarByClass(l34J6.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/data/students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/output/l34j6"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int[] monthCount = new int[13];
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                String phone = toks[5];
                if (phone.indexOf("00000") >= 0
                        || phone.indexOf("11111") >= 0
                        || phone.indexOf("22222") >= 0
                        || phone.indexOf("33333") >= 0
                        || phone.indexOf("44444") >= 0
                        || phone.indexOf("55555") >= 0
                        || phone.indexOf("66666") >= 0
                        || phone.indexOf("77777") >= 0
                        || phone.indexOf("88888") >= 0
                        || phone.indexOf("99999") >= 0
                ) {
                    context.write(new Text(toks[1]), new Text("1"));
                } else {
                    context.write(new Text(toks[1]), new Text("0"));
                }
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, Integer> clazz = new HashMap<>();
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                Integer val = Integer.parseInt(value.toString());
                if (clazz.containsKey(key.toString())) {
                    clazz.put(key.toString(),
                            clazz.get(key.toString()) + val);
                } else {
                    clazz.put(key.toString(), val);
                }
            }
            context.write(new Text(clazz.toString()), new Text(""));
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : clazz.entrySet()) {
                if (entry.getValue() == 0) {
                    context.write(new Text(entry.getKey()), new Text(""));
                }
            }
        }
    }
}
