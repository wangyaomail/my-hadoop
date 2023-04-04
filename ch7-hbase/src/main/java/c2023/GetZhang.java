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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class GetZhang {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.2");
        System.load("C:\\hadoop\\hadoop-3.2.2\\bin\\hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "zhang");
        job.setJarByClass(GetZhang.class);
        job.setNumReduceTasks(0);
        FileOutputFormat.setOutputPath(job, new Path("/getzhang"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("id"));
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("students"),
                scan,
                ZhangMapper.class,
                Text.class, Text.class,
                job
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class ZhangMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Cell nameCell = value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("name"));
            String name = Bytes.toString(CellUtil.cloneValue(nameCell));
            if(name.startsWith("张")){
                context.write(new Text(name), new Text(Bytes.toString(key.get())));
            }
        }
    }
}
