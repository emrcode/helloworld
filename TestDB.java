package sample_project;

//STEP 1. Import required packages package mariadb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class TestDB { // JDBC driver name and database URL
	
	private static String in_path = "";
	private static String dbname = "";
	/** The Log. */
	private static Logger Log = Logger.getLogger(ScriptExecuter.class);
	/*
	 * This Class is used to excute hql scripts.
	 */
	private static String hql_script_executer = Constants.hql_script_executer;
	
	//private static SparkConf conf = new SparkConf().setAppName("Simple Application");
	//private static JavaSparkContext jsc = new JavaSparkContext(conf);
	private static final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

    private static final SQLContext sqlContext = new SQLContext(sc);
	private static HiveContext hc = new HiveContext(sc.sc());
	private static SQLContext sqlcont = new SQLContext(sc.sc());
	private static Db mariaDB = new Db();
	//QueryUtils qu = new QueryUtils();
	private static Connection conn = null;
	private static Statement stmt = null;
	private static String sql = null;
	private static DataFrame dfmain = null;
	private static DataFrameReader dfread = null;
	
	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_USERNAME = "eclapp";
    private static final String MYSQL_PWD = "ZAQ!2wsxcde3";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://10.92.154.141:6603/ecldev?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;
	
	
	public static void main(String[] args) {
		MariaDBUtils mdb = new MariaDBUtils();
		mdb.readfromTable(sqlcont, "reftab_dbsic").show();
		//coonectotdb();
		//insertintomaridb();
		
		}
	
	public static void insertintomaridb(){
		
		Properties prop = new Properties();
				prop.setProperty("driver", "com.mysql.jdbc.Driver");
				prop.setProperty("user", "eclapp");
				prop.setProperty("password", "ZAQ!2wsxcde3"); 
		//dfmain = hqlExecuter(  hc,"/tables/employee3.hql");
		
				dfmain = hc.read().format("com.databricks.spark.csv")
						.option("inferSchema", "true").option("delimiter","|")
						.load("/user/ecldev/test/REFTAB_DBSIC.txt");
				
				dfmain.show();
		DataFrame dflatest = null;
		//DataFrame finalDF=null;
		/*
		String sql = "select * from sample";
		*/
		dflatest = dfmain.withColumnRenamed("C0","id").withColumnRenamed("C1","src_dbsic")
						 .withColumnRenamed("C2","src_dbsic_desc").withColumnRenamed("C3","tgt_broad_dbsic")
						 .withColumnRenamed("C4","tgt_broad_desc").withColumnRenamed("C5","tgt_sub_dbsic")
						 .withColumnRenamed("C6","tgt_sub_dbsic_desc").withColumnRenamed("C7","tgt_borrtype");
		
		/*
		 dfmain.write()
		 		.mode("append")
		 		.format("jdbc")
			  .option("url" ,"jdbc:mariadb://10.92.154.141:6603/ecldev")
			  .option("driver", "org.mariadb.jdbc.Driver")
			  .option("user", "eclapp")
			  .option("password", "ZAQ!2wsxcde3")
			  .option("dbtable","sample")
			  ;
		*/
		 //dfmain.insertIntoJDBC("jdbc:mariadb://10.92.154.141:6603/ecldev", "show_table", false);
		dflatest.write().mode("append").jdbc("jdbc:mysql://10.92.154.141:6603/ecldev","reftab_dbsic", prop);
		 //dfmain.write.mode("append").jdbc("jdbc:mariadb://10.92.154.141:6603/ecldev","show_table", prop);
		 
		/*
		dflatest = sqlContext.read()
					.format("jdbc")
				//.option("inferSchema", "true").option("header", "false")	
			  .option("url","jdbc:mysql://10.92.154.141:6603")
			  .option("driver", "com.mysql.jdbc.Driver")
			  .option("dbtable", "ecldev.sample")
			  .option("user", "eclapp")
			  .option("password", "ZAQ!2wsxcde3").load();
		
		
		Map<String, String> options = new HashMap<String, String>();
		 options.put("driver", MYSQL_DRIVER);
	        options.put("url", MYSQL_CONNECTION_URL);
//	        options.put("query", "select * from sample;");
	        options.put("dbtable","sample");
	      */             
		//DataFrame jdbcDF = sqlContext.read().format("jdbc").options(options).load();
			  
			 // .load();
		
//		spark.read
//		  .format("jdbc")
//		  .option("url", "jdbc:mysql:dbserver")
//		  .option("dbtable", “schema.tablename")
//		  .option("user", "username")
//		  .option("password", "password")
//		  .load()
		/*
		System.out.println("This is new Build");
		jdbcDF.printSchema();
		String[] Columns = jdbcDF.columns();
		for (String col : Columns){
			System.out.println(col);
		}
		jdbcDF.show();
		*/
//		finalDF=jdbcDF.select(jdbcDF.col("a"),jdbcDF.col("b"));
//		finalDF.show();
//		
		/*Row[] rd = dflatest.collect();
		String valueslist=null;
		for (int i=0;i< rd.length;i++){
			for(int j=0; j < rd[i].length();j++){
				System.out.print("--------"+rd[i].get(j));
				valueslist = valueslist + "," + rd[i].get(j);
			}
		}
		*/
		//hc.read().jdbc("jdbc:mariadb://10.92.154.141:6603/ecldev","sample", prop).show();
		//System.out.println(sqlcont.read().jdbc("jdbc:mariadb://10.92.154.141:6603/ecldev","show_table", prop).count());
		//dflatest.show();
		//dflatest.registerTempTable("test");
		//dflatest.sqlContext().sql("select * from test").show();
		//System.out.println(dfread.load().count());
		
		System.out.println("Goodbye!");
		System.out.println("New Goodbye!");
	}
		
		/*
		Row[] rd = dfmain.collect();
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("use ecldev");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String valueslist=null;
		for (int i=0;i< rd.length;i++){
			for(int j=0; j < rd[i].length();j++){
				System.out.print("--------"+rd[i].get(j));
				valueslist = valueslist + "," + rd[i].get(j);
			}
			System.out.println();
			valueslist = (String) valueslist.subSequence(1,valueslist.length());
			sql="insert into show_table values ("+valueslist+")";
			try {
				stmt.executeUpdate(sql);
				stmt.executeUpdate("commit");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		*/
		/*
		try {
			conn = mariaDB.getMariaDbConnection();
			System.out.println("Creating table in given database...");
			stmt = conn.createStatement();

			sql = "DROP TABLE table_list_hive ";

			stmt.executeUpdate("use ecldev");
			stmt.executeUpdate(sql);
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		}
		try {
			if (!mariaDB.closeDbConnection(conn)) {
				conn.close();
			}
		} catch (SQLException se) {
		}
		*/
	
	public static void coonectotdb(){
		try {
			conn = mariaDB.getMariaDbConnection();
			System.out.println("Creating table in given database...");
			stmt = conn.createStatement();

			//sql = "select * from show_table";
			sql = "select * from sample";
			
			//stmt.executeUpdate(sql);
			ResultSet rs = stmt.executeQuery(sql);
			
			while(rs.next()){
				System.out.println(rs.getString(1));
			}
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		}
		try {
			if (!mariaDB.closeDbConnection(conn)) {
				conn.close();
			}
		} catch (SQLException se) {
		}
	}
	public static DataFrame hqlExecuter( SQLContext HIVECONTEXT,String hqlScript) {
		//the argument hqlScript will take hql script to be executed.Please note that relative path with respect to the resources folder should be sent as parameter. ex : /tables/XXXXX.hql
		try {
			Constants.loadProperties();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			 //throw new RuntimeException(e); 
			Log.error(e.getCause());
		}
		in_path = Constants.in_path;
		dbname = Constants.DATABASE_NAME;
		DataFrame df = null;
		try
		{
			String hql_query="";
			String hql_query_final="";
			InputStream fis = null;
			fis = ScriptExecuter.class.getResourceAsStream(hqlScript);
			
			int content;
			hql_query = "";
			while ((content = fis.read() ) != -1) {
				hql_query = hql_query + String.valueOf((char) content);
				}
			hql_query_final=hql_query.replace("${hiveconf:db}",dbname).replace("${hiveconf:app_in_dir}",in_path);
			//System.out.println(sql_query_final);
			for( String statement : hql_query_final.split(";") ) {
				System.out.print( statement );
				df = HIVECONTEXT.sql(statement);
				}
			return df;
		}
		catch (Exception e) {
			 //throw new RuntimeException(e);
			Log.error(e.getCause());
			return df;
		}
	}

}
