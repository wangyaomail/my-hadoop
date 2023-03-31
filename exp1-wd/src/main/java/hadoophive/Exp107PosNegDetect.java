package hadoophive;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

public class Exp107PosNegDetect {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        {
            Job job = Job.getInstance(conf, "job");
            job.setJarByClass(Exp107PosNegDetect.class);
            job.setMapperClass(MyMapper1.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setReducerClass(MyReducer1.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path("/exp102"));
            FileOutputFormat.setOutputPath(job, new Path("/exp107"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    private static class MyMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {
        HashSet<String> posSet = new HashSet<>();
        HashSet<String> negSet = new HashSet<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();
            try{
                FileSystem fs = FileSystem.get(new URI("hdfs://zzti:9000"),conf);
                {
                    InputStream in = fs.open(new Path("/dict/正面词.dict"));
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        posSet.add(line.trim());
                    }
                    br.close();
                }
                {
                    InputStream in = fs.open(new Path("/dict/负面词.dict"));
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        negSet.add(line.trim());
                    }
                    br.close();
                }
//                fs.close();
            } catch(Exception e){
                e.printStackTrace();
            }
        }

        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            Result result = ToAnalysis.parse(toks[4]);
            int posNegScore = 0;
            for (Term term : result.getTerms()) {
                String word = term.getName();
                if (word.length() > 1) {
                    if (posSet.contains(word)) {
                        posNegScore++;
                    } else if (negSet.contains(word)) {
                        posNegScore--;
                    }
                }
            }
            context.write(new IntWritable(posNegScore), new Text(toks[1]+"\t"+toks[4]));
        }

    }

    private static class MyReducer1 extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, new Text(value));
            }
        }
    }

}
