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

public class PageRankV2CalculateB {
    public static void main(String[] args) throws Exception {
        String hadoopHome = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoopHome);
        System.load(hadoopHome + "\\bin\\hadoop.dll");

        Configuration conf = new Configuration();
        for (int i = 0; i < 10; i++) {
            Job job = Job.getInstance(conf, "myjob");
            job.setJarByClass(PageRankV2CalculateB.class);
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("C:\\Users\\wangy\\Desktop\\my-hadoop-maven-template\\test\\out" + i));
            FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\wangy\\Desktop\\my-hadoop-maven-template\\test\\out" + (i + 1)));
            if (job.waitForCompletion(true)) {
                continue;
            }
        }
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class WCMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                double stayScorePre = Double.parseDouble(toks[1]);
                String[] outLinks = toks[2].split(",");
                for (String out : outLinks) {
                    String[] linkScore = out.split(":");
                    double jumpScore = Double.parseDouble(linkScore[1]) * stayScorePre;
                    context.write(new Text(linkScore[0]), new Text("score\t" + jumpScore + ""));
                }
                context.write(new Text(toks[0]), new Text("edges\t" + toks[2]));
            }
        }

    }

    private static class WCReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double totalScore = 0;
            String edges = "";
            for (Text value : values) {
                String[] typeString = value.toString().split("\t");
                if (typeString[0].equals("score")) {
                    totalScore += Double.parseDouble(typeString[1].toString());
                } else if (typeString[0].equals("edges")) {
                    edges = typeString[1];
                }
            }
            if (edges.length() == 0) {
                edges = key + ":1.0";
            }
            context.write(key, new Text(totalScore + "\t" + edges));
        }
    }
}
