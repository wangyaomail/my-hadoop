package c202634;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HBasePut {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);

        List<Put> puts = new ArrayList<Put>();
        for(int i=0;i<20;i++){
            Put put = new Put(Bytes.toBytes(""+i));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("name"),
                    Bytes.toBytes("a"+i));
            puts.add(put);
        }

        Table table = conn.getTable(TableName.valueOf("students"));
        table.put(puts);







        conn.close();

    }
}
