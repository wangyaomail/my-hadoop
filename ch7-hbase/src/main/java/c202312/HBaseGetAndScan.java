package c202312;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseGetAndScan {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("s2");
        Table table = conn.getTable(tableName);

        Get get = new Get(Bytes.toBytes("100"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("gender"));
        get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("birthday"));
        Result rs = table.get(get);
        Cell cell = rs.getColumnLatestCell(Bytes.toBytes("data"), Bytes.toBytes("name"));
        System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
        System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
        System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

        System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("data"), Bytes.toBytes("gender"))));

        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes("15000"));
        scan.withStopRow(Bytes.toBytes("1501"));
        ResultScanner rsc = table.getScanner(scan);
        for(Result r :rsc){
            System.out.println(Bytes.toString(r.getRow()));
        }


        admin.close();
        conn.close();
    }
}
