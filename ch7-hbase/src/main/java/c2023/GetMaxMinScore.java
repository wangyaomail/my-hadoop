package c2023;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GetMaxMinScore {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.2");
        System.load("C:\\hadoop\\hadoop-3.2.2\\bin\\hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "maxminscore");
        job.setJarByClass(GetMaxMinScore.class);
//        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path("/maxminscore"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("students"),
                scan,
                MaxMinScoreMapper.class,
                Text.class, Text.class,
                job
        );
        job.setReducerClass(MaxMinScoreReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class MaxMinScoreMapper extends TableMapper<Text, Text> {
        int maxScore = 0;
        int minScore = 100;
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Cell scoreCell = value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("score"));
            int score = Integer.parseInt(Bytes.toString(CellUtil.cloneValue(scoreCell)));
            if (score < minScore) {
                minScore = score;
            }
            if (score > maxScore) {
                maxScore = score;
            }
        }
        @Override
        protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text(maxScore + ""));
            context.write(new Text(""), new Text(minScore + ""));
        }
    }

    private static class MaxMinScoreReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int maxScore = 0;
            int minScore = 100;
            for (Text value : values) {
                int score = Integer.parseInt(value.toString());
                if (score < minScore) {
                    minScore = score;
                }
                if (score > maxScore) {
                    maxScore = score;
                }
            }
            context.write(new Text("最大值"), new Text(maxScore + ""));
            context.write(new Text("最小值"), new Text(minScore + ""));
        }
    }
}
