package exp2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 单词数量统计
 */
public class Exp204WordCount {
    private static final byte[] _table_name_1 = Bytes.toBytes("exp201");
    private static final byte[] _table_name_2 = Bytes.toBytes("exp204");
    private static final byte[] _family = Bytes.toBytes("data");

    public static boolean checkAndCreateTable(byte[] table_name) {
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(table_name);
            if (!admin.tableExists(tableName)) {
                TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
                ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(_family).build();
                builder.setColumnFamily(familyDescriptor);
                TableDescriptor tableDescriptor = builder.build();
                admin.createTable(tableDescriptor);
            }
            conn.close();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
        System.setProperty("hadoop.home.dir", hadoop_home);
        System.load(hadoop_home + "/bin/hadoop.dll");
        if (checkAndCreateTable(_table_name_1) && checkAndCreateTable(_table_name_2)) {
            Configuration conf = HBaseConfiguration.create();
            Job job = Job.getInstance(conf, "myjob");
            job.setJarByClass(Exp204WordCount.class);
            job.setReducerClass(MyReducer.class);
            List<Scan> scans = new ArrayList<>();
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("toks"));
            scan.setCaching(200);
            scan.setCacheBlocks(false);
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name_1);
            scans.add(scan);
            TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
            TableMapReduceUtil.initTableReducerJob(new String(_table_name_2), MyReducer.class, job);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    static class MyMapper extends TableMapper<Text, Text> {

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            Cell cell = columns.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("toks"));
            if (cell != null) {
                String toks = Bytes.toString(CellUtil.cloneValue(cell));
                for (String tok : toks.split(",")) {
                    String[] wordCount = tok.split(":");
                    if(wordCount.length==2) {
                        context.write(new Text(wordCount[0]), new Text(wordCount[1]));
                    }
                }
            }
        }
    }

    static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count += Integer.parseInt(value.toString());
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.addColumn(_family,
                    Bytes.toBytes("word"),
                    Bytes.toBytes(count+""));
            context.write(new ImmutableBytesWritable(key.getBytes()), put);
        }
    }
}























































