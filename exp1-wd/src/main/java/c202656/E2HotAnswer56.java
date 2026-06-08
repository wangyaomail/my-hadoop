package c202656;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class E2HotAnswer56 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "lxk");
        job.setJarByClass(E2HotAnswer56.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,
                Bytes.toBytes("qa"));
        scans.add(scan);

        TableMapReduceUtil.initTableMapperJob(
                scans,
                MyMapper.class,
                Text.class,
                Text.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                "result",
                MyReducer.class,
                job
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static byte [] _family = Bytes.toBytes("data");
    static byte [] _star = Bytes.toBytes("star");
    static byte [] _title = Bytes.toBytes("title");
    static byte [] _birthday = Bytes.toBytes("birthday");
    static byte [] _gender = Bytes.toBytes("gender");
    static byte [] _loc = Bytes.toBytes("loc");
    static byte [] _score = Bytes.toBytes("score");
    static byte [] _clazz = Bytes.toBytes("clazz");
    static byte [] _phone = Bytes.toBytes("phone");


    static class MyMapper extends TableMapper<Text, Text> {
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell1 = columns.getColumnLatestCell(_family, _star);
            Cell cell2 = columns.getColumnLatestCell(_family, _title);
            if (cell1 != null && cell2 != null) {
                String star = Bytes.toString(CellUtil.cloneValue(cell1));
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell1));
                int starint = Integer.parseInt(star);
                if(starint>10000){
                    context.write(new Text(rowKey), new Text(starint+""));
                }
            }
        }

    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("data"),
                        Bytes.toBytes("star"),
                        Bytes.toBytes(value.toString()));
                context.write(new ImmutableBytesWritable(), put);
            }
        }
    }

}
