package c2023;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Hive1 {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String url = "jdbc:hive2://zzti2:10000/zzti";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        // 创建表
        stmt.execute("create table if not exists zzti.students(name string)");
        // 写入数据
        stmt.execute("insert into zzti.students values('zhangsan')");
        // 查询
        ResultSet res = stmt.executeQuery("select * from zzti.students");
        while (res.next()) {
            System.out.println(res.getString("name"));
        }
        conn.close();
    }
}
