package c202656;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBase4MR1 {
    public static void main(String[] args) throws Exception {

        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");


        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "mr1");
        job.setJarByClass(HBase4MR1.class);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("students"));
        scans.add(scan);

        job.setNumReduceTasks(0);

        TableMapReduceUtil.initTableMapperJob(scans,
                MyMapper.class,
                Text.class,Text.class,
                job);

        FileOutputFormat.setOutputPath(job, new Path("/data/output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static byte [] _family = Bytes.toBytes("data");
    static byte [] _q_name = Bytes.toBytes("name");
    static byte [] _q_sid = Bytes.toBytes("sid");


    static class MyMapper extends TableMapper<Text, Text> {
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell nameCell = columns.getColumnLatestCell(_family, _q_name);
            if (nameCell != null) {
                String name = Bytes.toString(CellUtil.cloneValue(nameCell));
                context.write(new Text(name), new Text(""));
            }
        }
    }

}
