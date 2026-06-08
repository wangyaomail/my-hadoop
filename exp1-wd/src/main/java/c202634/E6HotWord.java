package c202634;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.Reducer;
import util.CountMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class E6HotWord {
    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "aa");
        job.setJarByClass(E6HotWord.class);
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
                "result",
                MyReducer.class,
                job
        );
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static byte[] tableName = Bytes.toBytes("qa");
    static byte[] _family = Bytes.toBytes("data");
    static byte[] _q_content = Bytes.toBytes("content");
    static byte[] _q_qid = Bytes.toBytes("qid");
    static byte[] _q_star = Bytes.toBytes("star");

    static byte[] _q_tokens = Bytes.toBytes("tokens");
    static byte[] _q_cm = Bytes.toBytes("cm");
    static byte[] _q_pos = Bytes.toBytes("pos");
    static byte[] _q_neg = Bytes.toBytes("neg");
    static byte[] _q_good = Bytes.toBytes("good");


    static class MyMapper extends TableMapper<Text, Text> {
        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell1 = columns.getColumnLatestCell(_family, _q_cm);
            Cell cell2 = columns.getColumnLatestCell(_family, _q_qid);
            if (cell1 != null&& cell2 != null) {
                String cm = Bytes.toString(CellUtil.cloneValue(cell1));
                CountMap<String> countMap = CountMap.fromString(cm);
                if(countMap.size()>0){
                    for(String word : countMap.keySet()){
                        char c = word.charAt(0);
                        if (c >= 19968 && c <= 40869) { // 只要中文
                            context.write(new Text(word), new Text(countMap.get(word)+""));
                        }
                    }
                }
            }
        }

    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("sum"),
                    Bytes.toBytes(sum+""));
            context.write(new ImmutableBytesWritable(), put);

        }
    }

}
