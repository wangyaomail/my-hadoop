package ch7;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveTest {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");

        String url = "jdbc:hive2://zzti:10000/zzti";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("create table if not exists zzti.students(name string)");
        stmt.execute("insert into zzti.students values('zs')");
        stmt.execute("insert into zzti.students values('ls')");
        ResultSet res = stmt.executeQuery("select * from zzti.students");
        while (res.next()) {
            System.out.println(res.getString("name"));
        }
        conn.close();
    }
}
