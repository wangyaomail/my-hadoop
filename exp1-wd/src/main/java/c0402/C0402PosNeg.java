package c0402;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import util.CountMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Map;

public class C0402PosNeg {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(C0402PosNeg.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("token"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("qa"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("qa", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static String cell2String(Result value, String qualifier) {
        return Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes(qualifier))));
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        HashSet<String> posSet = new HashSet<>();
        HashSet<String> negSet = new HashSet<>();
        @Override
        protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();
            try {
                FileSystem fs = FileSystem.get(new URI("hdfs://zzti:9000"), conf);
                {
                    InputStream in = fs.open(new Path("/dict/正面词.dict"));
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        posSet.add(line.trim());
                    }
                    br.close();
                }
                {
                    InputStream in = fs.open(new Path("/dict/负面词.dict"));
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        negSet.add(line.trim());
                    }
                    br.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String token = cell2String(value, "token");
            if (StringUtils.isNoneEmpty(token)) {
                int pos = 0;
                CountMap<String> wordMap = CountMap.fromString(token);
                for (Map.Entry<String, Integer> entry : wordMap.entrySet()) {
                    if (posSet.contains(entry.getKey())) {
                        pos += entry.getValue();
                    } else if (negSet.contains(entry.getKey())) {
                        pos -= entry.getValue();
                    }
                }
                context.write(new Text(Bytes.toString(key.get())),
                        new Text(pos + ""));
            }
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("pos"),
                        Bytes.toBytes(val.toString()));
                context.write(new ImmutableBytesWritable(), put);
            }
        }
    }
}
