package ch4.l12;
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
 * 吉祥五连号
 */
public class l12j6 {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(l12j6.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,
                new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\lx12j6"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                if (toks[5].indexOf("00000") > -1
                        || toks[5].indexOf("11111") > -1
                        || toks[5].indexOf("22222") > -1
                        || toks[5].indexOf("33333") > -1
                        || toks[5].indexOf("44444") > -1
                        || toks[5].indexOf("55555") > -1
                        || toks[5].indexOf("66666") > -1
                        || toks[5].indexOf("77777") > -1
                        || toks[5].indexOf("88888") > -1
                        || toks[5].indexOf("99999") > -1) {
                    context.write(new Text(toks[1]), new Text("1"));
                } else {
                    context.write(new Text(toks[1]), new Text("0"));
                }
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, Integer> clazzCount = new HashMap<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text t : values) {
                count+=Integer.parseInt(t.toString());
            }
            clazzCount.put(key.toString(), count);
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(clazzCount.toString()), new Text(""));
            for (Map.Entry<String, Integer> entry : clazzCount.entrySet()) {
                if (entry.getValue() == 0) {
                    context.write(new Text(entry.getKey()), new Text(entry.getValue() + ""));
                }
            }
        }
    }
}
