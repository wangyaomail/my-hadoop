package c0402;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class c0402HotAnswerToFile {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(c0402HotAnswerToFile.class);
        job.setReducerClass(MyReducer.class);
        FileOutputFormat.setOutputPath(job, new Path("/hotanswer"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qid"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answer_id"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("star"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("qa"), scan, MyMapper.class, IntWritable.class, Text.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static String cell2String(Result value, String qualifier) {
        return Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes(qualifier))));
    }
    private static class MyMapper extends TableMapper<IntWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String qid = cell2String(value, "qid");
            String answer_id = cell2String(value, "answer_id");
            String title = cell2String(value, "title");
            String star = cell2String(value, "star");
            if (StringUtils.isNoneEmpty(qid, answer_id, title, star)) {
                context.write(new IntWritable(Integer.MAX_VALUE - Integer.parseInt(star)),
                        new Text(qid + "-" + answer_id + "\t" + title));
            }
        }
    }

    private static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
        public void reduce(
                IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int realStar = Integer.MAX_VALUE - key.get();
            for (Text val : values) {
                context.write(new Text(realStar + ""), val);
            }
        }
    }
}
