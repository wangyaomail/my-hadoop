package c202556;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Job5PosNeg {
    static byte[] tableName = Bytes.toBytes("qa");
    static byte[] _family = Bytes.toBytes("data");
    static byte[] _q_content = Bytes.toBytes("content");
    static byte[] _q_sid = Bytes.toBytes("sid");
    static byte[] _q_tokens = Bytes.toBytes("tokens");
    static byte[] _q_pos = Bytes.toBytes("pos");
    static byte[] _q_neg = Bytes.toBytes("neg");
    static byte[] _q_good = Bytes.toBytes("good");


    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(Job5PosNeg.class);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans,
                                            MyMapper.class,
                                            Text.class,
                                            Text.class,
                                            job
        );

        TableMapReduceUtil.initTableReducerJob("qa",
                                            MyReducer.class,
                                            job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static class MyMapper extends TableMapper<Text, Text> {
        HashSet<String> posSet = new HashSet<>();
        HashSet<String> negSet = new HashSet<>();
        @Override
        protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = new Configuration();
            try {
                FileSystem fs = FileSystem.get(new URI("hdfs://zzti:9000"), conf);
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell = columns.getColumnLatestCell(_family, _q_tokens);
            if (cell != null) {
                int posNum=0,negNum=0;
                for(String tok: Bytes.toString(CellUtil.cloneValue(cell)).split(",")) {
                    if(posSet.contains(tok)) {
                        posNum++;
                    } else if(negSet.contains(tok)) {
                        negNum++;
                    }
                }
                context.write(new Text(Bytes.toString(CellUtil.cloneRow(cell))),
                        new Text(posNum+","+negNum));
            }
        }
    }
    static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for(Text value : values){
                String[] toks = value.toString().split(",");
                int pos = Integer.parseInt(toks[0]);
                int neg = Integer.parseInt(toks[1]);
                int good = pos>neg?1:0;
                Put put = new Put(Bytes.toBytes(key.toString()));
                put.addColumn(Bytes.toBytes("data"),
                        _q_pos,
                        Bytes.toBytes(pos+""));
                put.addColumn(Bytes.toBytes("data"),
                        _q_neg,
                        Bytes.toBytes(neg+""));
                put.addColumn(Bytes.toBytes("data"),
                        _q_good,
                        Bytes.toBytes(good+""));

                context.write(new ImmutableBytesWritable(),put);
            }

        }
    }
}
