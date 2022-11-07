package ch4.s202202;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
/**
 * 统计入边
 */
public class ch4c3PageRankV2 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "r1");
        job.setJarByClass(ch4c3PageRankV2.class);
        job.setMapperClass(JobMapper1.class);
        job.setReducerClass(JobReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                                     new Path("E:\\data\\knowledge_graph\\freebase\\Release\\1_train.txt"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch403v1r20"));
        if (job.waitForCompletion(true)) {
            int round = 3;
            for (int i = 0; i < round; i++) {
                job = Job.getInstance(conf, "r2");
                job.setJarByClass(ch4c3PageRankV2.class);
                job.setMapperClass(JobMapper2.class);
                job.setReducerClass(JobReducer2.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/ch403v1r2" + i));
                int j=i+1;
                FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch403v1r2" + j));
                if(job.waitForCompletion(true)){
                    continue;
                }
            }
            job = Job.getInstance(conf, "r3");
            job.setJarByClass(ch4c3PageRankV2.class);
            job.setMapperClass(JobMapper3.class);
            job.setReducerClass(JobReducer3.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(localProjectPath + "/test/ch403v1r2" + round));
            FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/ch403v1r3"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
    private static class JobMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                context.write(new Text(toks[0]), new Text(toks[2]));
            }
        }
    }
    private static class JobReducer1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Set<String> edges = new HashSet<>();
            for (Text value : values) {
                edges.add(value.toString().trim());
            }
            double p = 1.0 / edges.size();
            StringBuilder sb = new StringBuilder();
            sb.append("1\t");
            for (String e : edges) {
                sb.append(e).append(":").append(p).append(",");
            }
            context.write(key, new Text(sb.substring(0, sb.length() - 1)));
        }
    }
    private static class JobMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                double stayP = Double.parseDouble(toks[1]);
                if (toks[2] != null && toks[2].length() > 0) {
                    for (String evs : toks[2].split(",")) {
                        String[] ev = evs.split(":");
                        String name = ev[0];
                        String p = ev[1];
                        double jumpP = stayP * Double.parseDouble(p);
                        context.write(new Text(name), new Text("score" + "\t" + jumpP));
                    }
                }
            }
            context.write(new Text(toks[0]), new Text("edge" + "\t" + toks[2]));
        }
    }
    private static class JobReducer2 extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String edgeName = null;
            double stayP = 0;
            for (Text value : values) {
                String[] typeVal = value.toString().trim().split("\t");
                if (typeVal[0].equals("score")) {
                    stayP += Double.parseDouble(typeVal[1]);
                } else if (typeVal[0].equals("edge")) {
                    edgeName = typeVal[1];
                }
            }
            if (edgeName == null) {
                edgeName = key.toString()+":1.0";
            }
            context.write(key, new Text(stayP + "\t" + edgeName));
        }
    }
    private static class JobMapper3 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if (toks.length == 3) {
                context.write(new DoubleWritable(Double.parseDouble(toks[1])), new Text(toks[0]));
            }
        }
    }
    private static class JobReducer3 extends Reducer<DoubleWritable, Text, Text, Text> {
        public void reduce(
                DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, new Text(key.toString()));
            }
        }
    }
}
