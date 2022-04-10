package ch4;

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
import java.util.HashSet;
import java.util.Set;

public class PageRankV2CalculateA {
    public static void main(String[] args) throws Exception {
        String hadoopHome = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopHome);
        System.load(hadoopHome + "\\bin\\hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(PageRankV2CalculateA.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("D:\\data\\knowledge_graph\\freebase\\Release\\1_train.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\wangy\\Desktop\\my-hadoop-maven-template\\test\\out0"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class WCMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                context.write(new Text(toks[0]), new Text(toks[2]));
            }
        }

    }

    private static class WCReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> edges = new HashSet<>();
            for (Text value : values) {
                edges.add(value.toString());
            }
            double p = 1.0 / edges.size();
            StringBuilder sb = new StringBuilder();
            sb.append(1).append("\t");
            for (String edge : edges) {
                sb.append(edge).append(":").append(p).append(",");
            }
            context.write(key, new Text(sb.substring(0, sb.length() - 1)));
        }
    }
}
