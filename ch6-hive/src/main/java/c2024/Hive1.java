package c2024;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Hive1 {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        String url = "jdbc:hive2://192.168.17.150:10000/zzti";
        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
//        stmt.execute("create table if not exists zzti.students(name string)");

//        stmt.execute("insert into zzti.students values('zhangsan')");

        ResultSet rs = stmt.executeQuery("select * from zzti.stu2");
        while (rs.next()) {
            System.out.println(rs.getString("name"));
            System.out.println(rs.getString("score"));
        }

        conn.close();
    }
}
