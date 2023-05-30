package c202334;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseMR2 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(HBaseMR2.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("students"),
                scan,
                MyMapper.class,
                Text.class, Text.class,
                job);
        TableMapReduceUtil.initTableReducerJob("students",
                MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        int maxScore = -Integer.MAX_VALUE, minScore = Integer.MAX_VALUE;
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String scoreStr = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("score"))));
            int score = Integer.parseInt(scoreStr);
            if (score > maxScore) {
                maxScore = score ;
            }
            if (score < minScore) {
                minScore = score;
            }
        }
        @Override
        protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text("1"),new Text(maxScore+""));
            context.write(new Text("1"),new Text(minScore+""));
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int maxScore = -Integer.MAX_VALUE, minScore = Integer.MAX_VALUE;
            for (Text val : values) {
                int score = Integer.parseInt(val.toString());
                if (score > maxScore) {
                    maxScore = score ;
                }
                if (score < minScore) {
                    minScore = score;
                }
            }
            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("maxmin"), Bytes.toBytes(maxScore+""));
            context.write(new ImmutableBytesWritable(), put);
            put = new Put(Bytes.toBytes("2"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("maxmin"), Bytes.toBytes(minScore+""));
            context.write(new ImmutableBytesWritable(), put);
        }
    }
}
