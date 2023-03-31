package hadoophive;

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

/**
 * 打印前三行
 */
public class Exp101PreprocessA {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Index");
        job.setJarByClass(Exp101PreprocessA.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path("E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_valid.json"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/exp101"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        int count=0;
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if(count++<3){
                context.write(new Text(count+""), value);
            }
        }
    }
}
