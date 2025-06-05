package c202578;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.BaseAnalysis;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Job3HotWordHBase {

    static byte[] _table_name = Bytes.toBytes("j23");
    static byte[] _family = Bytes.toBytes("data");
    static byte[] _q_title = Bytes.toBytes("title");
    static byte[] _q_content = Bytes.toBytes("content");
    static byte[] _q_star = Bytes.toBytes("star");
    static byte[] _q_count = Bytes.toBytes("count");


    public static void main(String[] args) throws Exception {

        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");

        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(Job3HotWordHBase.class);

//        job.setReducerClass(MyReducer.class);

        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name);
        scans.add(scan);
        TableMapReduceUtil.initTableMapperJob(scans,
                                            MyMapper.class,
                                            Text.class,
                                            Text.class,
                                            job);
        TableMapReduceUtil.initTableReducerJob("hotword",
                                            MyReducer.class,
                                            job);
//        FileOutputFormat.setOutputPath(job, new Path("/hotword"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    static class MyMapper extends TableMapper<Text, Text> {
        @Override
        protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            BaseAnalysis.parse("你好");
        }

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell scoreCell = columns.getColumnLatestCell(_family, _q_content);
            if (scoreCell != null) {
                String content = Bytes.toString(CellUtil.cloneValue(scoreCell));
                org.ansj.domain.Result result = BaseAnalysis.parse(content);
                for(Term term : result.getTerms()) {
                    context.write(new Text(term.getName()), new Text(""));
                }
            }
        }
    }
    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for(Text value : values) {
                count++;
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(_family, _q_count, Bytes.toBytes(count+""));
            context.write(new ImmutableBytesWritable(), put);
        }

    }

}
