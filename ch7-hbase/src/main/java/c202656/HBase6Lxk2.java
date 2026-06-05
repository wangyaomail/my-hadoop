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
import java.util.*;

public class HBase6Lxk2 {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "lxk");
        job.setJarByClass(HBase6Lxk2.class);
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
                "result",
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
        List<String> jxhList = new ArrayList<>();
        {
            jxhList.add("111");
            jxhList.add("222");
            jxhList.add("333");
            jxhList.add("444");
            jxhList.add("555");
            jxhList.add("666");
            jxhList.add("777");
            jxhList.add("888");
            jxhList.add("999");
            jxhList.add("000");
        }
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell1 = columns.getColumnLatestCell(_family, _clazz);
            Cell cell2 = columns.getColumnLatestCell(_family, _phone);
            if (cell1 != null && cell2 != null) {
                String phone = Bytes.toString(CellUtil.cloneValue(cell2));
                String clazz = Bytes.toString(CellUtil.cloneValue(cell1));

                for(String jxh : jxhList){
                    if(phone.contains(jxh)){
                        context.write(new Text(clazz), new Text("1"));
                        break;
                    }
                }
            }
        }

    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count++;
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("count"),
                    Bytes.toBytes(count+""));
            context.write(new ImmutableBytesWritable(), put);
        }
    }

}
