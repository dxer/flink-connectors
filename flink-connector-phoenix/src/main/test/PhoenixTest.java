import org.apache.flink.connector.phoenix.Constans;

import java.sql.*;

public class PhoenixTest {


    public static void main(String[] args) throws Exception {

        Class.forName(Constans.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(String.format(Constans.PHOENIX_JDBC_URL_TEMPLATE, "http://127.0.0.1:8765"));

        connection.setAutoCommit(true);

        String query = "select * from user_phoenix";
        String upsertSQL = "UPSERT INTO user_phoenix (id,name,age) VALUES (?,?,?)";
        String delSQL = "DELETE FROM user_phoenix WHERE id=?";


        Statement statement1 = connection.createStatement();
        System.out.println(statement1.executeUpdate("UPSERT INTO user_phoenix(id,name,age) VALUES(12345, 'zhangsan', 123) "));
     //   connection.commit();

//        PreparedStatement upsertPstmt = connection.prepareStatement(upsertSQL);
//        upsertPstmt.setObject(1, 11);
//        upsertPstmt.setObject(2, "zhangsan111");
//        upsertPstmt.setObject(3, 123);
//        System.out.println(upsertPstmt.executeUpdate());

//        PreparedStatement delPstmt = connection.prepareStatement(delSQL);
//        delPstmt.setObject(1, 1);
//        System.out.println(delPstmt.executeUpdate());

        // connection.commit();


//        Statement statement = connection.createStatement();
//
//        ResultSet resultSet = statement.executeQuery(query);
//        while (resultSet.next()) {
//            System.out.println(resultSet.getInt("id") + "\t" + resultSet.getString("name") + "\t" + resultSet.getInt("age"));
//        }

    }
}
