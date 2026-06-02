package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// 计算年龄s
public class HBase4MR2 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "mr");
        job.setJarByClass(HBase4MR2.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes("students"));
        scans.add(scan);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                MyMapper.class,
                Text.class,
                Text.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "students",
                MyReducer.class,
                job
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static byte [] _family = Bytes.toBytes("data");
    static byte [] _sid = Bytes.toBytes("sid");
    static byte [] _name = Bytes.toBytes("name");
    static byte [] _birthday = Bytes.toBytes("birthday");
    static byte [] _gender = Bytes.toBytes("gender");
    static byte [] _loc = Bytes.toBytes("loc");
    static byte [] _score = Bytes.toBytes("score");
    static byte [] _clazz = Bytes.toBytes("clazz");
    static byte [] _phone = Bytes.toBytes("phone");


    static class MyMapper extends TableMapper<Text, Text> {
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell = columns.getColumnLatestCell(_family, _birthday);
            if (cell != null) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String birthday = Bytes.toString(CellUtil.cloneValue(cell));
                int age = 2126 - Integer.parseInt(birthday.substring(0, 4));
                context.write(new Text(rowKey), new Text(age+""));
            }
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            String age = values.iterator().next().toString();
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("sid"),
                    Bytes.toBytes(age));
            context.write(new ImmutableBytesWritable(), put);

            put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("birthday"),
                    Bytes.toBytes(age));
            context.write(new ImmutableBytesWritable(), put);

            put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("loc"),
                    Bytes.toBytes(age));
            context.write(new ImmutableBytesWritable(), put);

            put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("score"),
                    Bytes.toBytes(age));
            context.write(new ImmutableBytesWritable(), put);

            put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("gender"),
                    Bytes.toBytes(age));
            context.write(new ImmutableBytesWritable(), put);
        }
    }

}
