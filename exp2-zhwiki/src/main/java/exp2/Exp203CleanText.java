package exp2;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.apache.hadoop.conf.Configuration;
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
import org.nlpcn.commons.lang.jianfan.JianFan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 将正文字段进行清洗，去除英文，重新写回原表
 */
public class Exp203CleanText {
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
            scan.addColumn(Bytes.toBytes("data"), Bytes.toBytes("revision_text"));
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
        NlpAnalysis analysis = new NlpAnalysis();

        protected void map(ImmutableBytesWritable key, Result columns, Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
            Cell cell = columns.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("revision_text"));
            if (cell != null) {
                String textSrc = Bytes.toString(CellUtil.cloneValue(cell));
                StringBuilder sb = new StringBuilder();
                for (char c : textSrc.toCharArray()) {
                    if (c >= 19968 && c <= 40869) { // 只要中文
                        sb.append(c);
                    }
                }
                String sjt = JianFan.f2j(sb.toString()); // 简繁转换
                // 将简体的字段存入HBase
                {
                    Put put = new Put(columns.getRow());
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("content"), Bytes.toBytes(sjt));
                    context.write(key, put);
                }
                org.ansj.domain.Result result = analysis.parseStr(sjt); // 分词，统计词频并转换
                // 统计次数，并输出
                CountMap<String> countMap = new CountMap<>();
                for (Term term : result) {
                    String tok = term.getName();
                    if (tok.length() < 10 && tok.length() > 1) { // 太长的词不要，单个字不要
                        countMap.add(tok);
                    }
                }
                String countMapOutput = countMap.toSortedString(true);
                {
                    Put put = new Put(columns.getRow());
                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("toks"), Bytes.toBytes(countMapOutput));
                    context.write(key, put);
                }
            }
        }

    }

}























































