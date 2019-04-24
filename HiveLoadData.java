package sample;

import java.sql.*;
import java.util.Scanner;

public class HiveLoadData {
    private static String getDBName() {
        Scanner sc = new Scanner(System.in);
        String db = null;
        System.out.println("Enter the name of database");
        db = sc.next();
        return db;
    }

    private static String getTableName() {
        Scanner sc = new Scanner(System.in);
        String tableName= null;
        System.out.println("Enter the name of table");
        tableName = sc.next();
        return tableName;
    }

    public static void main(String[] args) throws SQLException {
        try {
            Class.forName("org.apache.hive:hive-jdbc");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
        Statement stmt = conn.createStatement();
        String databaseName = getDBName();
        stmt.execute("CREATE DATABASE " + databaseName);
        System.out.println("Database " + databaseName + " created successfully.");
        stmt.execute("USE " + databaseName);
        String tableName = getTableName();
        stmt.executeQuery("drop table "+tableName);
        //Create table query
/*
        String sql = "CREATE TABLE IF NOT EXISTS "+tableName+ "(year int,month int,day_of_month int," +
                "day_of_week int,dep_time string,crs_dep_time string,arr_time string,crs_arr_time string," +
                "unique_carrier string,flight_num string,tail_num string,actual_elapsed_time int,crs_elapsed_time int," +
                "air_time int,arr_delay int,dep_delay int,origin string,dest string," +
                "distance int,taxi_in int,taxi_out int,cancelled int," +
                "cancellation_code string,diverted int,carrier_delay int,weather_delay int,nas_delay int,security_delay int," +
                "late_aircraft_delay int) " +
                " row format delimited fields terminated by '" + "," + "'" +
                " lines terminated by '" + "\\n" + "' TBLPROPERTIES (\"skip.header.line.count\"=\"1\")";
        stmt.execute(sql);
        System.out.println(String.format("Table %s created.",tableName));
    */
        //show tables

        /*String showTable="show tables '"+tableName+"'";
        System.out.println("Running "+showTable);
        ResultSet res=stmt.executeQuery(showTable);
        if(res.next()){
            System.out.println(res.getString(1));
        }*/

        //describe table

       /* String descTable="describe "+tableName;
        System.out.println("Running: " +descTable);
        res=stmt.executeQuery(descTable);
        while(res.next()){
            System.out.println(res.getString(1)+"\t"+res.getString(2)+"\t"+res.getString(3));
        }*/


        //load data into table

        String filepath = "/home/sample/Customers.txt";
        String loadQuery = "LOAD DATA LOCAL INPATH '" + filepath + "' into table " + tableName;
        stmt.execute(loadQuery);
        System.out.println(" Running " + loadQuery);
        System.out.println(String.format("Load Data into %s successful", tableName));
        //res.close();
        stmt.close();
        conn.close();
    }
}
