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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 扫描全表，拿到所有的列信息
 */
public class Exp202ShowColumn {
    private static final byte[] _table_name_1 = Bytes.toBytes("exp201");
    private static final byte[] _table_name_2 = Bytes.toBytes("exp202");
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
            job.setJarByClass(Exp202ShowColumn.class);
            job.setReducerClass(MyReducer.class);
            List<Scan> scans = new ArrayList<>();
            Scan scan = new Scan();
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
        ArrayList<String> qualList = new ArrayList<>();

        protected void map(ImmutableBytesWritable key, Result columns, Context context) throws IOException, InterruptedException {
            for (Cell cell : columns.rawCells()) {
                String qual = Bytes.toString(CellUtil.cloneQualifier(cell));
                qualList.add(qual);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("cols"),
                    new Text(String.join(",", qualList.stream().distinct().collect(Collectors.toList()))));
        }
    }

    static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> qualSet = new HashSet<>();
            for (Text t : values) {
                qualSet.addAll(Arrays.asList(t.toString().split(",")));
            }
            for (String qual : qualSet) {
                try{
                    Put put = new Put(Bytes.toBytes(qual));
                    put.addColumn(_family,
                            Bytes.toBytes("col"),
                            Bytes.toBytes(qual));
                    context.write(new ImmutableBytesWritable(key.getBytes()), put);
                } catch(Exception e){
                    e.printStackTrace();
                }
            }
        }
    }
}























































