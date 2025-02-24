package exp2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * 计算文章正负面倾向
 * 计算结果写入原表，以一个新的字段存在
 */
public class Exp206PosNegDetect {
    private static final byte[] _table_name_1 = Bytes.toBytes("exp201");
    private static final byte[] _table_name_2 = Bytes.toBytes("exp201");
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
            job.setJarByClass(Exp203CleanText.class);
            job.setNumReduceTasks(0);
            List<Scan> scans = new ArrayList<>();
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("toks"));
            scan.setCaching(200);
            scan.setCacheBlocks(false);
            scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, _table_name_1);
            scans.add(scan);
            TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, ImmutableBytesWritable.class, Mutation.class, job);
            job.setOutputFormatClass(TableOutputFormat.class);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Bytes.toString(_table_name_2));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

    static class MyMapper extends TableMapper<ImmutableBytesWritable, Mutation> {
        public HashSet<String> posSet = new HashSet<>();
        public HashSet<String> negSet = new HashSet<>();
        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = new Configuration();
            FileSystem fs = null;
            try {
                fs = FileSystem.get(new URI("hdfs://zzti:9000"), conf);
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
                fs.close();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            System.out.println(posSet.size() + ":" + negSet.size());
        }

        protected void map(ImmutableBytesWritable key, Result columns, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Cell cell = columns.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("toks"));
            if (cell != null) {
                int pos_neg = 0;
                String toks = Bytes.toString(CellUtil.cloneValue(cell));
                for (String tok : toks.split(",")) {
                    String[] wordCount = tok.split(":");
                    if (wordCount.length == 2) {
                        if (posSet.contains(wordCount[0])) {
                            pos_neg++;
                        } else if (negSet.contains(wordCount[0])) {
                            pos_neg--;
                        }
                    }
                }
                Put put = new Put(columns.getRow());
                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("pos_neg"), Bytes.toBytes(pos_neg+""));
                context.write(key, put);
            }
        }
    }

}























































