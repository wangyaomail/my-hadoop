package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseStart {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        Get get = new Get(Bytes.toBytes("1"));

        Table table = conn.getTable(TableName.valueOf("students"));
        Result rs = table.get(get);

        String name = Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("name")));
        String sid = Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("sid")));
        String gender = Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("gender")));
        System.out.println(name+" "+sid+" "+gender);


        conn.close();

    }
}
