package ch4.s202202;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
/**
 * 倒排索引
 */
public class ch4c2InvertInedxArticle {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(ch4c2InvertInedxArticle.class);
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
//        for (File dir : new File("C:\\Users\\d\\Desktop\\20news-18828").listFiles()) {
//            FileInputFormat.addInputPath(job, new Path(dir.getAbsolutePath()));
//        }
        FileInputFormat.addInputPath(job, new Path("C:\\Users\\d\\Desktop\\20news-18828\\comp.os.ms-windows.misc"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch4c2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(" ");
            FileSplit fileInput = (FileSplit) context.getInputSplit();
            String fileName = fileInput.getPath().getName();
            for (String tok : toks) {
                context.write(new Text(tok), new Text(fileName + ""));
            }
        }
    }
    private static class JobReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value).append(",");
            }
            context.write(key, new Text(sb.toString()));
        }
    }
}
