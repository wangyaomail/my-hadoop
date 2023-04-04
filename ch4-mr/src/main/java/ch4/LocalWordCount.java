//package ch4;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Iterator;
//
//public class LocalWordCount {
//    public static void main(String[] args) throws Exception{
//        String localHadoopHome = "C:\\hadoop\\hadoop-3.2.2";
//        System.setProperty("hadoop.home.dir", localHadoopHome);
//        System.load(localHadoopHome + "/bin/hadoop.dll");
//
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "wordcount");
//        job.setJarByClass(LocalWordCount.class);
//        job.setMapperClass(LWCMapper.class);
//        job.setReducerClass(LWCReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path("C:\\nos\\my-hadoop\\data\\the_old_man_and_sea.txt"));
//        FileOutputFormat.setOutputPath(job, new Path("C:\\nos\\my-hadoop\\output\\lwc"));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//    }
//
//    private static class LWCMapper extends Mapper<LongWritable, Text, Text, Text> {
//        public HashMap<String, Integer> wordCountMap;
//        @Override
//        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            wordCountMap = new HashMap<>();
//        }
//        public void map(
//                LongWritable key, Text value, Context context)
//                throws IOException, InterruptedException {
//            String[] toks = value.toString().trim().split(" ");
//            for (int i = 0; i < toks.length; i++) {
////                context.write(new Text(toks[i]), new Text("1"));
//                if(wordCountMap.containsKey(toks[i])){
//                    wordCountMap.put(toks[i], wordCountMap.get(toks[i])+1);
//                } else{
//                    wordCountMap.put(toks[i], 1);
//                }
//            }
//        }
//        @Override
//        protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            for(xx entry: wordCountMap.entrySet()){
//                String key = entry.getKey();
//                String value = entry.getValue();
//                context.write(new Text(key), new Text(value));
//            }
//        }
//    }
//
//    private static class LWCReducer extends Reducer<Text, Text, Text, Text> {
//        public void reduce(
//                Text key, Iterable<Text> values, Context context)
//                throws IOException, InterruptedException {
//            int count = 0;
//            for (Text value : values) {
//                count+= Integer.parseInt(value.toString());
//            }
//            context.write(key, new Text(count + ""));
//        }
//    }
//
//}
