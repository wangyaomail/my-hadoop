package c0402;
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
import util.FixedTreeMap;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public class C0402HotWord {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(C0402HotWord.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("qa"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("result", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static String cell2String(Result value, String qualifier) {
        return Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes(qualifier))));
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        private CountMap<String> wordMap = new CountMap<>();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String content = cell2String(value, "content");
            if (StringUtils.isNoneEmpty(content)) {
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
            for (Map.Entry<String, Integer> entry : wordMap.cut(100, true).entrySet()) {
                context.write(new Text(entry.getKey()),
                        new Text("" + entry.getValue()));
            }
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        private CountMap<String> wordMap = new CountMap<>();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                wordMap.add(key.toString(), Integer.parseInt(val.toString()));
            }
        }
        @Override
        protected void cleanup(Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Put put = new Put(Bytes.toBytes("hotword"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("result"),
                    Bytes.toBytes(wordMap.cut(10, true).toString()));
            context.write(new ImmutableBytesWritable(), put);
        }
    }
}
