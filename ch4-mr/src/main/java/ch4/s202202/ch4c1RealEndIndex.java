package ch4.s202202;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
/**
 * 真实结束位置
 */
public class ch4c1RealEndIndex {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(ch4c1RealEndIndex.class);
        job.setMapperClass(JobMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path(localProjectPath + "/data/the_old_man_and_sea.txt"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch4c1"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        int count =0;
        HashSet<String> toksFiltered = new HashSet<>();
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            count++;
            String[] toks = value.toString().trim().split(" ");
            if (toks.length > 0) {
                for (String tok : toks) {
                    toksFiltered.add(tok);
                    if (tok.endsWith(".") || tok.endsWith("?")) {
                        context.write(new Text(count + ""), new Text(toksFiltered + ""));
                        toksFiltered.clear();
                    }
                }
            }
        }
    }
}
