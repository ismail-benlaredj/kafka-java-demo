package com.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        Properties propsDB = new Properties();
        propsDB.setProperty("user", "postgres");
        propsDB.setProperty("password", "i8s18");
        // propsDB.setProperty("ssl", "true");

        Connection conn = DriverManager.getConnection(url, propsDB);
        Statement statement = conn.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM kafka");

        // Process the resultSet if needed
        while (resultSet.next()) {
            String columnValue = resultSet.getString("mean");
            System.out.println("Column Value: " + columnValue);
        }

        // Close resources
        resultSet.close();
        statement.close();
        conn.close();
    }
}
