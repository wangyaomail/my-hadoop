package c202578;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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

public class CH703Job4 {

    static byte[] _table_name = Bytes.toBytes("students");
    static byte[] _family = Bytes.toBytes("data");
    static byte[] _q_name = Bytes.toBytes("name");
    static byte[] _q_sid = Bytes.toBytes("sid");


    public static void main(String[] args) throws Exception {

        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(CH703Job4.class);
        job.setNumReduceTasks(0);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);

        FileOutputFormat.setOutputPath(job, new Path("/ch721"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static class MyMapper extends TableMapper<Text, Text> {
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell nameCell = columns.getColumnLatestCell(_family, _q_name);
            Cell sidCell = columns.getColumnLatestCell(_family, _q_sid);
            if (nameCell != null && sidCell != null) {
                String name = Bytes.toString(CellUtil.cloneValue(nameCell));
                String sid = Bytes.toString(CellUtil.cloneValue(sidCell));
                if (name.startsWith("张")) {
                    context.write(new Text(name), new Text(sid));
                }
            }
        }
    }

}
