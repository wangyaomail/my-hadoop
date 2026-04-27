package ch4.c2026.c56;
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
import java.util.List;

public class Job20260427L5焦作前三2 {

    static class Two{
        public String name;
        public Integer score;
        public Two(String name, Integer score) {
            this.name = name;
            this.score = score;
        }
    }
    static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        List<Two> list= new ArrayList<>();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split("\t");
            if(toks.length==8){
                if (toks[6].contains("焦作")){
                    list.add(new Two(toks[0], Integer.parseInt(toks[7])));
                }
            }
        }

        @Override
        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            list.sort(new Comparator<Two>() {
                @Override
                public int compare(Two o1, Two o2) {
                    return o1.score.compareTo(o2.score);
                }
            });
            for (int i=0;i<3;i++) {
                context.write(new Text(""),new Text(list.get(i).name+"\t"+list.get(i).score));
            }
        }
    }

    static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            List<Two> list= new ArrayList<>();
            for (Text val : values) {
                String[] name_score =  val.toString().split("\t");
                list.add(new Two(name_score[0], Integer.parseInt(name_score[1])));
            }
            list.sort(new Comparator<Two>() {
                @Override
                public int compare(Two o1, Two o2) {
                    return o1.score.compareTo(o2.score);
                }
            });
            for (int i=0;i<3;i++) {
                context.write(new Text(i+""),new Text(list.get(i).name+"\t"+list.get(i).score));
            }
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, Job20260427L5焦作前三2.class.getSimpleName());
        job.setJarByClass(Job20260427L5焦作前三2.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\students_10w.data"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\"+ Job20260427L5焦作前三2.class.getSimpleName()));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
