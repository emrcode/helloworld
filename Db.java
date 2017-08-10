package sample_project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Db {

	private final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
	private final String DB_URL = "jdbc:mariadb://192.168.100.174/ecldev";
	
	//  Database credentials
	private String USER = "root";
	private String PASS = "root";
	
	/*
	 * public String getpwd(){
	 
		
	}
	*/
	public Connection getMariaDbConnection(){
		Connection conn = null;
	    try {
	    	Class.forName("org.mariadb.jdbc.Driver");

	        //STEP 3: Open a connection
	        System.out.println("Connecting to selected database...");
	        //conn = DriverManager.getConnection("jdbc:mariadb://10.92.154.141:6603/db", "eclapp/ZAQ!2wsxcde3", "ecldev");
	        conn = DriverManager.getConnection("jdbc:mariadb://10.92.154.141:6603/ecldev/?user=eclapp&password=ZAQ!2wsxcde3");
	        System.out.println("Successfully Connected to selected database...");
	    	}
	    catch (SQLException se) {
	        //Handle errors for JDBC
	        se.printStackTrace();
	    	}
	    catch (Exception e) {
	        //Handle errors for Class.forName
	        e.printStackTrace();
	    	}

		return conn;
	
	}
	
	public boolean closeDbConnection(Connection conn){
		try {
            if (conn != null) {
                conn.close();
            	}
        	} 
		catch (SQLException se) {
            return false;
			//se.printStackTrace();
            
        	}//end finally try
		return true;
	}

}