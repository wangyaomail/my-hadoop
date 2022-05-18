//package ch7;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//import org.apache.hadoop.hbase.util.Bytes;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//
//public class CH706ImportStudentsData {
//    public static void main(String[] args) throws  Exception {
//        String hadoop_home = "C:\\hadoop\\hadoop-3.x\\hadoop-3.2.2";
//        System.setProperty("hadoop.home.dir", hadoop_home);
//        System.load(hadoop_home + "/bin/hadoop.dll");
//        Configuration conf = HBaseConfiguration.create();
//        Connection conn = ConnectionFactory.createConnection(conf);
//        Admin admin = conn.getAdmin();
//
//
//        Table table = conn.getTable(TableName.valueOf("students"));
//        if(false) {
//            BufferedReader br = new BufferedReader(new FileReader("data/students_10w.data"));
//            String line = null;
//            while ((line = br.readLine()) != null) {
//                String[] toks = line.trim().split("\t");
//                if (toks.length == 8) {
//                    Put put = new Put(Bytes.toBytes(toks[2]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes(toks[0]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("clazz"), Bytes.toBytes(toks[1]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("sid"), Bytes.toBytes(toks[2]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"), Bytes.toBytes(toks[3]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"), Bytes.toBytes(toks[4]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("phone"), Bytes.toBytes(toks[5]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("loc"), Bytes.toBytes(toks[6]));
//                    put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes(toks[7]));
//                    table.put(put);
//                    System.out.println(toks[2]);
//                }
//            }
//        }
//        Put put = new Put(Bytes.toBytes("RB0058155"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"), System.currentTimeMillis()+24l*3600*1000, Bytes.toBytes("鄂锋破"));
//        put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("aa"), 1, Bytes.toBytes("123"));
//        table.put(put);
//
//        conn.close();
//    }
//}
