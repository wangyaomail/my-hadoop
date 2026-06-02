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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class HBase5MRMore3 {
    static byte[] _family = Bytes.toBytes("data");
    static byte[] _sid = Bytes.toBytes("sid");
    static byte[] _name = Bytes.toBytes("name");
    static byte[] _birthday = Bytes.toBytes("birthday");
    static byte[] _clazz = Bytes.toBytes("clazz");
    static byte[] _gender = Bytes.toBytes("gender");
    static byte[] _loc = Bytes.toBytes("loc");
    static byte[] _phone = Bytes.toBytes("phone");
    static byte[] _score = Bytes.toBytes("score");
    public static void main(String[] args) throws Exception {

        String hadoop_home = "C:\\hadoop\\hadoop-3.2.2";
        System.setProperty("HADOOP_USER_NAME", "zzti");
        System.setProperty("hadoop.user.name", "zzti");
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");


        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "mr1");
        job.setJarByClass(HBase5MRMore3.class);
        job.setMapperClass(MyMapper.class);
        List<Scan> scans = new ArrayList<Scan>();
        Scan scan = new Scan();
//        scan.addColumn(_family, _birthday);
        scan.setCaching(200);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("students"));
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
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class MyMapper extends TableMapper<Text, Text> {
        HashMap<String, Integer> dayCount = new HashMap<>();
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            Cell cell = value.getColumnLatestCell(_family, _birthday);
            if (cell!= null) {
                String birthday = Bytes.toString(CellUtil.cloneValue(cell));
                String day = birthday.substring(birthday.length()-2);
                if(dayCount.containsKey(day)){
                    dayCount.put(day, dayCount.get(day) + 1);
                } else {
                    dayCount.put(day, 1);
                }
            }
        }

        @Override
        protected void cleanup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException {
            for(Map.Entry entry : dayCount.entrySet()){
                context.write(new Text("1"),new Text(entry.getKey()+":"+entry.getValue().toString()));
            }
        }
    }

    private static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        TreeMap<String,Integer> dayCount = new TreeMap<>(
                new Comparator<String>() {
                    public int compare(String s1, String s2) {
                        return s2.compareTo(s1);
                    }
                }
        );
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            for(Text value : values){
                String[] day_count = value.toString().split(":");
                dayCount.put(day_count[0],Integer.parseInt(day_count[1]));
            }
            List<Map.Entry<String, Integer>> list = new ArrayList<>(dayCount.entrySet());
            list.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });
            int count = 0;
            for(Map.Entry entry : list){
                count++;
//                if(count++<3){
                    Put put = new Put(Bytes.toBytes("birthdaySort"));
                    put.addColumn(_family,
                            Bytes.toBytes(count+""),
                            Bytes.toBytes(entry.getKey()+":"+entry.getValue().toString()));
                    context.write(new ImmutableBytesWritable(), put);
//                }
            }

        }
    }
}
