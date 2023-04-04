package ch7;
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

public class CH723AgeGenerateToSrc {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(CH723AgeGenerateToSrc.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("students"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("students", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String birthday = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            Integer year = Integer.parseInt(birthday.split("-")[0]);
            Integer age = 2022 - year;
            context.write(new Text(Bytes.toString(value.getRow())), new Text(age + ""));
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("ageageage"), Bytes.toBytes(values.iterator().next().toString()));
            context.write(new ImmutableBytesWritable(), put);
        }
    }
}
