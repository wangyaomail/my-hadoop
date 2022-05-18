package ch7;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class CH711GenderCount extends CH701HBaseBase {
    @Override
    public void run() throws IOException {
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("data"),Bytes.toBytes("gender"));
        Table table = conn.getTable(TableName.valueOf("students"));
        ResultScanner resultScanner = table.getScanner(scan);
        int maleCount=0,femaleCount=0;
        for(Result result : resultScanner){
            String gender = Bytes.toString(result.getValue(Bytes.toBytes("data"),Bytes.toBytes("gender")));
            if(gender.equals("男")){
                maleCount++;
            } else {
                femaleCount++;
            }
        }
        System.out.println("男生人数："+maleCount);
        System.out.println("女生人数："+femaleCount);

        conn.close();
    }

    public static void main(String[] args) throws IOException {
        new CH711GenderCount().run();

    }
}
