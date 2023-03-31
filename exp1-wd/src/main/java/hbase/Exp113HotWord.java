package hbase;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;

/**
 * 热点回答，HBase版
 * 返回前10最热的词
 */
public class Exp113HotWord {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(Exp113HotWord.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"));

        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("qa"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("result", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends TableMapper<Text, Text> {
        private CountMap<String> wordMap = new CountMap<>();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String content = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("content"))));
            if (StringUtils.isNotEmpty(content)) {
                org.ansj.domain.Result result = ToAnalysis.parse(content);
                for (Term term : result.getTerms()) {
                    if (term.getName().length() > 1) {
                        wordMap.add(term.getName());
                    }
                }
            }
        }

        @Override
        protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            wordMap.cut(10, true).forEach((key, value) -> {
                try {
                    context.write(new Text(""), new Text(key + ";;" + value));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            CountMap<String> wordMap = new CountMap<>();
            for (Text val : values) {
                String[] keyvals = val.toString().split(";;");
                wordMap.add(keyvals[0], Integer.parseInt(keyvals[1]));
            }
            Put put = new Put(Bytes.toBytes("hotword"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("val"), Bytes.toBytes(wordMap.cut(10, true).toString()));
            context.write(new ImmutableBytesWritable(), put);
        }
    }
}
