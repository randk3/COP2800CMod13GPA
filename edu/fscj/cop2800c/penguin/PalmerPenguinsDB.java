// PenguinRookeryDB.java
// Kristina Randolph
// 4/26/26
// Class for Penguin Rookery DB operations

package edu.fscj.cop2800c.penguin;

import java.sql.*;
import java.util.ArrayList;

public class PalmerPenguinsDB
{
    public static void createDB(ArrayList<Penguin> penguins) {
        final String DB_NAME = "PalmerPenguins";
        final String CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        final String CONN_URL = "jdbc:sqlserver://localhost:1433;integratedSecurity=true;";
        final String SQL_DROP_TABLE = "DROP TABLE Penguin";

        try {
            Class.forName(CLASS_NAME);

            try (Connection con = DriverManager.getConnection(CONN_URL);
                 Statement stmt = con.createStatement()) {

                // Ensure DB does NOT already exist (so we never print "already exists")
                stmt.executeUpdate("USE master");
                try {
                    stmt.executeUpdate(
                        "IF DB_ID('" + DB_NAME + "') IS NOT NULL " +
                        "BEGIN " +
                        "  ALTER DATABASE " + DB_NAME + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " +
                        "  DROP DATABASE " + DB_NAME + "; " +
                        "END"
                    );
                } catch (SQLException ignore) {
                    // ignore
                }

                // Create DB (will succeed now)
                stmt.executeUpdate("CREATE DATABASE " + DB_NAME);
                System.out.println("DB created");

                // Switch context to the new DB
                stmt.executeUpdate("USE " + DB_NAME);

                // Create table
                // *** add your code here
                String createTable = "CREATE TABLE Penguin "
                        + "(SAMPLENUM smallint PRIMARY KEY NOT NULL,"
                        + "CULMENLEN float NOT NULL,"
                        + "CULMENDEPTH float NOT NULL,"
                        + "BODYMASS smallint NOT NULL,"
                        + "SEX char(1) NOT NULL,"
                        + "SPECIES varchar(20) NOT NULL,"
                        + "FLIPPERLEN float NOT NULL)";

                stmt.executeUpdate(createTable);
                System.out.println("Table created");

                // Insert records using batch with try-with-resources
                String insertQuery = "INSERT INTO Penguin (SAMPLENUM, CULMENLEN, CULMENDEPTH, " +
                                     "BODYMASS, SEX, SPECIES, FLIPPERLEN) VALUES (?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement pstmt = con.prepareStatement(insertQuery)) {
                    for (Penguin penguin : penguins) {
                        pstmt.setInt(1, penguin.getSampleNum());
                        pstmt.setDouble(2, penguin.getCulmenLength());
                        pstmt.setDouble(3, penguin.getCulmenDepth());
                        pstmt.setInt(4, (int) penguin.getBodyMass());

                        String sex = penguin.getSex();
                        pstmt.setString(5, (sex != null && !sex.isEmpty()) ? sex.substring(0, 1) : "?");

                        // Store SPECIES in uppercase to match expected output (ADELIE, CHINSTRAP, GENTOO)
                        pstmt.setString(6, penguin.getSpecies().toString().toUpperCase());

                        pstmt.setDouble(7, penguin.getFlipperLength());

                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();

                    // IMPORTANT: remove this line for exact expected output
                    // System.out.println("Data inserted");
                }

                // Query and print results using try-with-resources
                try (ResultSet rs = stmt.executeQuery("SELECT * FROM Penguin ORDER BY SAMPLENUM")) {
                    // *** add your code here
                    System.out.println("Reading from DB");
                    while (rs.next()) {
                        System.out.println(
                                rs.getInt("SAMPLENUM") + "," +
                                rs.getDouble("CULMENLEN") + "," +
                                rs.getDouble("CULMENDEPTH") + "," +
                                rs.getInt("BODYMASS") + "," +
                                rs.getString("SEX") + "," +
                                rs.getString("SPECIES") + "," +
                                rs.getDouble("FLIPPERLEN")
                        );
                    }
                }

                // Drop the table
                stmt.executeUpdate(SQL_DROP_TABLE);
                System.out.println("Penguin table dropped");

                try {
                    stmt.executeUpdate("DROP DATABASE " + DB_NAME + ";");
                    System.out.println("DB dropped");
                } catch (SQLException e) {
                    // The PDF says to ignore this if you see it
                    System.out.println("could not drop DB, in use");
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}