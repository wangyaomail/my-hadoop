package c2023;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
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

public class CalAgeScore {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-3.2.2");
        System.load("C:\\hadoop\\hadoop-3.2.2\\bin\\hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "calage");
        job.setJarByClass(CalAgeScore.class);
//        job.setNumReduceTasks(0);
//        FileOutputFormat.setOutputPath(job, new Path("/maxminscore"));
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("students"),
                scan,
                CalAgeMapper.class,
                Text.class, Text.class,
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                "students",
                CalAgeReducer.class,
                job
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class CalAgeMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Cell scoreCell = value.getColumnLatestCell(Bytes.toBytes("data"),
                    Bytes.toBytes("birthday"));
            Cell nameCell = value.getColumnLatestCell(Bytes.toBytes("data"),
                    Bytes.toBytes("name"));
            String bstr = Bytes.toString(CellUtil.cloneValue(scoreCell));
            String namestr = Bytes.toString(CellUtil.cloneValue(nameCell));
            int age = 2023 - Integer.parseInt(bstr.substring(0, 4));
            context.write(new Text(Bytes.toString(key.get())),
                    new Text(age + "-" + namestr));
        }
    }

    private static class CalAgeReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Put put = new Put(key.copyBytes());
            String[] val = values.iterator().next().toString().split("-");

            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("age"),
                    Bytes.toBytes(val[0]));
//            put.addColumn(Bytes.toBytes("data"),
//                    Bytes.toBytes("name"),
//                    Bytes.toBytes(val[1]));
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
        }
    }
}
