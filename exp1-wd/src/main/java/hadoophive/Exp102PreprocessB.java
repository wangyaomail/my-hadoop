package hadoophive;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.Sbc2Dbc;

import java.io.File;
import java.io.IOException;

public class Exp102PreprocessB {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Index");
        job.setJarByClass(Exp102PreprocessB.class);
        job.setNumReduceTasks(0);
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        String localProjectPath = new File("").getAbsolutePath();
        FileInputFormat.addInputPath(job,
                new Path("E:\\data\\nlp_chinese_corpus\\webtext2019zh\\web_text_zh_valid.json"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/test/exp102"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(
                LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            JSONObject json = JSONObject.parseObject(value.toString());
            Integer qid = json.getInteger("qid");
            Integer star = json.getInteger("star");
            Integer answer_id = json.getInteger("answer_id");
            String title = cleanInputString(json.getString("title"));
            String desc = cleanInputString(json.getString("desc"));
            String topic = json.getString("topic");
            String content = cleanInputString(json.getString("content"));
            String answerer_tags = cleanInputString(json.getString("answerer_tags"));
            context.write(new Text(qid + "\t"
                    + title + "\t"
                    + star + "\t"
                    + topic + "\t"
                    + content + "\t"
                    + answer_id + "\t"
                    + answerer_tags + "\t"
                    + desc
            ), NullWritable.get());
        }

        public String cleanInputString(String input) {
            if (input != null && input.length() > 0) {            // 去掉空格、换行、回车
                input = input.replaceAll("[\t\n ]+", "");            // 标点符号转全角，避免和程序中的字符冲突
                input = Sbc2Dbc.ToSBCWithoutLetterNumSpace(input);
            }
            return input;
        }
    }


}
