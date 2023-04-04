package c0402;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.FixedTreeMap;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

public class C0402HotAnswer {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "myjob");
        job.setJarByClass(C0402HotAnswer.class);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qid"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("answer_id"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"));
        scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("star"));
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("qa"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob("result", MyReducer.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    private static String cell2String(Result value, String qualifier) {
        return Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes(qualifier))));
    }
    private static class MyMapper extends TableMapper<Text, Text> {
        public FixedTreeMap<Integer, String> fixedMap = new FixedTreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return -o1 + o2;
            }
        }, 100);
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            String qid = cell2String(value, "qid");
            String answer_id = cell2String(value, "answer_id");
            String title = cell2String(value, "title");
            String star = cell2String(value, "star");
            if (StringUtils.isNoneEmpty(qid, answer_id, title, star)) {
                fixedMap.put(Integer.parseInt(star), qid + "-" + answer_id);
            }
        }
        @Override
        protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, String> entry : fixedMap.entrySet()) {
                context.write(new Text("1"), new Text(entry.getKey() + ";;;" + entry.getValue()));
            }
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            FixedTreeMap<Integer, String> fixedMap = new FixedTreeMap<>(new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    return -o1 + o2;
                }
            }, 100);
            for (Text val : values) {
                String[] toks = val.toString().split(";;;");
                fixedMap.put(Integer.parseInt(toks[0]), toks[1]);
            }
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Integer, String> entry : fixedMap.entrySet()) {
                sb.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }
            Put put = new Put(Bytes.toBytes("hotanswer"));
            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("result"), Bytes.toBytes(sb.toString()));
            context.write(new ImmutableBytesWritable(), put);
        }
    }
}
