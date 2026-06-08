package c202656;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import util.CountMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class E6PosNeg {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "lxk");
        job.setJarByClass(E6PosNeg.class);
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
                "qa",
                MyReducer.class,
                job
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static byte [] _family = Bytes.toBytes("data");
    static byte [] _star = Bytes.toBytes("star");
    static byte [] _title = Bytes.toBytes("title");
    static byte [] _qid = Bytes.toBytes("qid");
    static byte [] _content = Bytes.toBytes("content");
    static byte [] _tokens = Bytes.toBytes("tokens");
    static byte [] _cm = Bytes.toBytes("cm");
    static byte [] _clazz = Bytes.toBytes("clazz");
    static byte [] _phone = Bytes.toBytes("phone");


    static class MyMapper extends TableMapper<Text, Text> {
        HashSet<String> posSet = new HashSet<>();
        HashSet<String> negSet = new HashSet<>();
        @Override
        protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            {
                InputStream in = fs.open(new Path("/正面词.dict"));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = null;
                while ((line = br.readLine()) != null) {
                    posSet.add(line.trim());
                }
                br.close();
            }
            {
                InputStream in = fs.open(new Path("/负面词.dict"));
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line = null;
                while ((line = br.readLine()) != null) {
                    negSet.add(line.trim());
                }
                br.close();
            }
        }

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell1 = columns.getColumnLatestCell(_family, _cm);
            Cell cell2 = columns.getColumnLatestCell(_family, _qid);
            if (cell1 != null && cell2 != null) {
                int pos = 0;
                String cmstr = Bytes.toString(CellUtil.cloneValue(cell1));
                CountMap<String> cm = CountMap.fromString(cmstr);
                for(String word: cm.keySet()){
                    if(posSet.contains(word)){
                        pos += cm.get(word);
                    }
                    if(negSet.contains(word)){
                        pos -= cm.get(word);
                    }
                }
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell1));
                context.write(new Text(rowkey), new Text(pos+""));
            }
        }

    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("data"),
                        Bytes.toBytes("pos"),
                        Bytes.toBytes(value.toString()));
                context.write(new ImmutableBytesWritable(), put);
            }
        }
    }

}
