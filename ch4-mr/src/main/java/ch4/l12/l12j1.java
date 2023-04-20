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
import sun.security.krb5.internal.crypto.EType;

import java.io.IOException;

/**
 * 70分以上的比70分以下的多几个？
 */
public class l12j1 {
    public static void main(String[] args) throws Exception {
        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", localHadoopHome);
        System.load(localHadoopHome + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(l12j1.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,
                new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job,
                new Path("C:\\nos\\my-hadoop\\output\\lx12j1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 8) {
                int score = Integer.parseInt(toks[7]);
                if (score > 70) {
                    context.write(new Text(">70"), new Text(score + ""));
                } else {
                    context.write(new Text("<70"), new Text(score + ""));
                }
            }
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        int above70 = 0, below70 = 0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                if (key.toString().equals(">70")) {
                    above70++;
                } else {
                    below70++;
                }
            }
        }
        @Override
        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(">70"), new Text(above70+""));
            context.write(new Text("<70"), new Text(below70+""));
            context.write(new Text("大于70的人数比小于70的人多"), new Text((above70 - below70) + ""));
        }
    }
}
