package sample_project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.Log;

public class MariaDBUtils {
	private static  Logger Log = Logger.getLogger(ScriptExecuter.class);
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "eclapp";
    private static final String MYSQL_DBNAME = "ecldev";
    private static final String MYSQL_PWD = "ZAQ!2wsxcde3";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://10.92.154.141:6603/"+ MYSQL_DBNAME +"?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
    private static DataFrame df = null;
    
	
	private static Connection getMariaDbConnection(){
		Connection conn = null;
	    try {
	    	Class.forName(MYSQL_DRIVER);
	    	Log.info("Connecting to database..." + MYSQL_DBNAME);
	    	conn = DriverManager.getConnection("jdbc:mariadb://10.92.154.141:6603/ecldev/?user=eclapp&password=ZAQ!2wsxcde3");
	    	Log.info("Successfully Connected to " + MYSQL_DBNAME);
	    	}
	    catch (SQLException se) {
	        //Handle errors for JDBC
	    	Log.error(se.getCause());
	    	}
	    catch (Exception se) {
	        //Handle errors for Class.forName
	    	Log.error(se.getCause());
	    	}

		return conn;
	
	}
	
	private boolean closeDbConnection(Connection conn){
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
	
	public static void writetoTable(DataFrame dfvar,String table){
		  
		 dfvar.write()
		 	  .mode("append")
		 	  .format("jdbc")
			  .option("url" ,MYSQL_CONNECTION_URL)
			  .option("driver", MYSQL_DRIVER)
			  //.option("user", "eclapp")
			  //.option("password", "ZAQ!2wsxcde3")
			  .option("dbtable",table)
			  ;
	}
	
	public static DataFrame readfromTable(SQLContext sql , String table){
		Map<String, String> options = new HashMap<String, String>();
		 	options.put("driver", MYSQL_DRIVER);
	        options.put("url", MYSQL_CONNECTION_URL);
	        options.put("dbtable",table);
	                   
		DataFrame jdbcDF = sql.read().format("jdbc").options(options).load();
		return jdbcDF;
	}
	
	
}
