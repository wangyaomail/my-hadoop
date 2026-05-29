package c202634;

import c202312.HBaseMR2;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBase4MR2 {
    public static void main(String[] args) throws Exception {

        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.user.name", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");


        Configuration conf = HBaseConfiguration.create();

        Job job = Job.getInstance(conf, "mr1");

        job.setJarByClass(HBase4MR2.class);
        job.setMapperClass(MyMapper.class);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));

        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("students"));
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
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static byte[] _family = Bytes.toBytes("data");
    static byte[] _q_name = Bytes.toBytes("name");
    static byte[] _birthday = Bytes.toBytes("birthday");

    private static class MyMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Cell nameCell = value.getColumnLatestCell(_family, _q_name);
            Cell birthCell = value.getColumnLatestCell(_family, _birthday);
            if (nameCell != null&&birthCell != null) {
                String name = Bytes.toString(CellUtil.cloneValue(nameCell));
                String birthday = Bytes.toString(CellUtil.cloneValue(birthCell));
                context.write(new Text(name), new Text(birthday));
            }
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("year"), Bytes.toBytes(values.iterator().next().toString()));
            context.write(new ImmutableBytesWritable(), put);
        }
    }
}
