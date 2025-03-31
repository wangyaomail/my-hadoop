package ch4.c2025;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;

public class Job2025C78PR2 {
    static DecimalFormat df = new DecimalFormat("#0.000000");
    static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==3){
                context.write(new Text(toks[0]), new Text(toks[2]));
            }
        }
    }

    static class MyReducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            HashSet<String> edges = new HashSet<>();
            for (Text val : values) {
                edges.add(val.toString());
            }
            String p =df.format(1/(double)edges.size());
            StringBuilder sb = new StringBuilder();
            for( String edge : edges ){
                sb.append(edge).append(":").append(p).append(",");
            }
            context.write(key, new Text("1\t"+sb.substring(0, sb.length()-1)));
        }
    }

    static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==3){
                double stayProb = Double.parseDouble(toks[1]);
                if (toks[2] != null && toks[2].length() > 0) {
                    for (String edge : toks[2].split(",")) {
                        String[] edgeNameProb = edge.split(":");
                        double jumbProb = stayProb * Double.parseDouble(edgeNameProb[1]);
                        context.write(new Text(edgeNameProb[0]),
                                new Text("score" + "\t" + jumbProb));
                    }
                }
                context.write(new Text(toks[0]), new Text("edge" + "\t" + toks[2]));            }
        }
    }

    static class MyReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String outLink = null;
            double stayProb = 0;
            for (Text value : values) {
                String[] typeVal = value.toString().split("\t");
                if (typeVal.length == 2) {
                    if (typeVal[0].equals("edge")) {
                        outLink = typeVal[1];
                    } else if (typeVal[0].equals("score")) {
                        stayProb += Double.parseDouble(typeVal[1]);
                    }
                }
            }
            if (outLink == null) { // 处理“黑洞”
                outLink = key + ":1.000000";
            }
            context.write(key, new Text(df.format(stayProb) + "\t" + outLink));
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2025C78PR2.class.getSimpleName()+"1");
        job.setJarByClass(Job2025C78PR2.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\share\\data\\FB15K-237.2\\Release\\train.txt"));
        String output1 = "C:\\nos\\my-hadoop\\output\\"+ Job2025C78PR2.class.getSimpleName()+".0";
        String output2 = "C:\\nos\\my-hadoop\\output\\"+ Job2025C78PR2.class.getSimpleName();
        FileOutputFormat.setOutputPath(job, new Path(output1));
        if(job.waitForCompletion(true)){
            for(int i=0;i<3;i++){
                job = Job.getInstance(conf, Job2025C78PR2.class.getSimpleName()+"2");
                job.setJarByClass(Job2025C78PR2.class);
                job.setMapperClass(MyMapper2.class);
                job.setReducerClass(MyReducer2.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(output2+"."+i));
                FileOutputFormat.setOutputPath(job, new Path(output2+"."+(i+1)));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
            }
        }

    }
}
