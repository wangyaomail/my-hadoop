package ch4.c2025.lxk56;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * 省人三多
 */
public class Job2025l3 {
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        HashMap<String, Integer> smap = new HashMap<>();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==8){
                String sss = toks[6].substring(0,3);
                if(smap.containsKey(sss)){
                    smap.put(sss, smap.get(sss)+1);
                }else{
                    smap.put(sss, 1);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            for( Map.Entry<String, Integer> entry : smap.entrySet()){
                context.write(new Text(""), new Text(entry.getKey()+":"+entry.getValue()));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            HashMap<String, Integer> smap = new HashMap<>();
            for (Text val : values) {
                String[] sss = val.toString().split(":");
                if(smap.containsKey(sss[0])){
                    smap.put(sss[0], smap.get(sss)+Integer.parseInt(sss[1]));
                }else{
                    smap.put(sss[0], Integer.parseInt(sss[1]));
                }
            }
            ArrayList<Map.Entry<String, Integer>> sortlist = new ArrayList<>(smap.entrySet());
            sortlist.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return  - o1.getValue() + o2.getValue();
                }
            });
            context.write(new Text("第三大"), new Text(sortlist.get(2).getKey()+":"+sortlist.get(2).getValue()));
            for(Map.Entry<String, Integer> entry : sortlist){
                context.write(new Text(entry.getKey()), new Text(entry.getValue()+""));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job2025l3.class.getSimpleName());
        job.setJarByClass(Job2025l3.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job2025l3.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
