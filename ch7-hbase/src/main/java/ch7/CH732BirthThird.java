package ch7;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CH732BirthThird {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(CH732BirthThird.class);
        job.setReducerClass(MyReducer.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        FileOutputFormat.setOutputPath(job, new Path("/birththird"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("students"), scan, MyMapper.class, Text.class, Text.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        HashMap<String, Integer> birthCount = new HashMap<>();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String birthday = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("birthday"))));
            String day = birthday.split("-")[2];
            if (birthCount.containsKey(day)) {
                birthCount.put(day, birthCount.get(day) + 1);
            } else {
                birthCount.put(day, 1);
            }
        }
        @Override
        protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, Integer> entry : birthCount.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            context.write(new Text("1"), new Text(sb.substring(0, sb.length() - 1)));
        }
    }

    private static class MyReducer extends Reducer<Text, Text, Text, Text> {
        HashMap<String, Integer> birthCount = new HashMap<>();
        public void reduce(
                Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                String[] dayCounts = value.toString().split(",");
                for (String dayCountStr : dayCounts) {
                    String[] dayCount = dayCountStr.split(":");
                    if (birthCount.containsKey(dayCount[0])) {
                        birthCount.put(dayCount[0], birthCount.get(dayCount[0]) + Integer.parseInt(dayCount[1]));
                    } else {
                        birthCount.put(dayCount[0], Integer.parseInt(dayCount[1]));
                    }
                }
            }
            context.write(new Text(""), new Text(birthCount.toString()));
        }
    }
}
