package sample_project;

import org.apache.hadoop.fs.FileSystem;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

//import com.dbs.ifrs.reportgeneration.*;

import scala.Tuple2;
//import ifrs.constants.ReportGeneratorConstants;

import java.util.*;
import java.io.*;

public class Sample {
	final static SparkConf SPARKCONF = new SparkConf()
			.setAppName("Hive Utility");
	final static JavaSparkContext JAVACONTEXT = new JavaSparkContext(SPARKCONF);
	final static HiveContext HIVECONTEXT = new HiveContext(JAVACONTEXT.sc());
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {

		String table_script = "";
		String table_path = "../resources/tables/employee3.hql";
		String sql_query = "";
		
		  try { Class.forName(driverName); } catch (ClassNotFoundException e) {
		  // TODO Auto-generated catch block e.printStackTrace();
		  System.exit(1); }
		  
		  
		  //FileSystem fs = FileSystem.get(JAVACONTEXT.hadoopConfiguration());
		  Connection con = DriverManager.getConnection(
		  "jdbc:hive2://10.92.139.143:10000/nrd_app_ecl;principal=hive/x01shdpeapp1a.sgp.dbs.com@REG1.UAT1BANK.DBS.COM");
		  Statement stmt = con.createStatement();
		 
		File dir = new File(table_path);
		File[] directoryListing = dir.listFiles();

		if (directoryListing != null) {
			for (File child : directoryListing) {

				FileInputStream fis = null;

				try {
					fis = new FileInputStream(child);

					// System.out.println("Total file size to read (in bytes) : "+
					// fis.available());

					int content;
					sql_query = "";
					while ((content = fis.read()) != -1) {
						// convert to char and display it
						sql_query = sql_query + String.valueOf((char) content);

					}
					System.out.println(sql_query);
					// stmt.executeQuery(sql_query);
					HIVECONTEXT.sql(sql_query);
					DataFrame df = HIVECONTEXT.sql(sql_query);

				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						if (fis != null)
							fis.close();
					} catch (IOException ex) {
						ex.printStackTrace();
					}
				}
			}

		} else {
			System.out
					.println("No Scripts are there for Hive Tables to be created");
		}

	
		 stmt.close(); con.close();
		
		/*
		 * End of Logic to handle Hive table creation when we the script in
		 * server under a given path
		 */
		
		/*  try{ 
			  Process p = Runtime.getRuntime().exec((new
		  String[]{"/usr/bin/beeline ","-u","jdbc:hive2://10.92.139.143:10000/nrd_app_ecl;principal=hive/x01shdpeapp1a.sgp.dbs.com@REG1.UAT1BANK.DBS.COM -f /ECLDEV/app/scripts/hql/test_hive_table/ecl9_accounts_emr.hql"
		  })); } catch (IOException e) { e.printStackTrace(); }
		 
		/*
		 * try{ Process p = Runtime.getRuntime().exec((new
		 * String[]{"sh /ECLDEV/app/scripts/shell/run_hql.sh"
		 * ,"ecl9_accounts_emr"})); } catch (IOException e) {
		 * e.printStackTrace(); }
		 */
		/*
		 * HIVECONTEXT. try { String[] command={"/bin/bash",
		 * "run_hql.sh ecl9_accounts_emr","/ECLDEV/app/scripts/shell/"}; Process
		 * awk = new ProcessBuilder().start(); awk.waitFor(); } catch
		 * (IOException e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } catch (InterruptedException e) {
		 * e.printStackTrace(); }
		 */
/*
		String[] command = { "/bin/bash", "run_hql.sh", "ecl9_accounts_emr" };

		try {
			ProcessBuilder p = new ProcessBuilder("sh", "run_hql.sh",
					"ecl9_accounts_emr");
			Process p2 = p.start();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					p2.getInputStream()));
			String line;

			System.out.println("Output of running " + "/bin/bash"
					+ "run_hql.sh" + "ecl9_accounts_emr" + " is: ");
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		          try {
                        String target = new String("/ECLDEV/app/scripts/shell/run_hql.sh");
// String target = new String("mkdir stackOver");
                        Runtime rt = Runtime.getRuntime();
                        Process proc = rt.exec(target);
                        proc.waitFor();
                        StringBuffer output = new StringBuffer();
                        BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
                        String line = "";                       
                        while ((line = reader.readLine())!= null) {
                                output.append(line + "\n");
                        }
                        System.out.println("### " + output);
                } catch (Throwable t) {
                        t.printStackTrace();
                }


	// Call hive.
		// System.out.println(String.format("Calling hive using %s.",
		// HQL_PATH));
		/*
		 * ProcessBuilder hiveProcessBuilder = new ProcessBuilder("hive", "-f",
		 * table_path); Process hiveProcess = hiveProcessBuilder.start();
		 * 
		 * OutputRedirector outRedirect = new
		 * OutputRedirector(hiveProcess.getInputStream(), "HIVE_OUTPUT");
		 * OutputRedirector outToConsole = new
		 * OutputRedirector(hiveProcess.getErrorStream(), "HIVE_LOG");
		 * 
		 * outRedirect.start(); outToConsole.start();
		 * 
		 * hiveProcess.waitFor();
		 * System.out.println(String.format("Hive job call completed."));
		 * hiveProcess.destroy();
		 * 
		 * } catch (ClassNotFoundException e) { e.printStackTrace(); } catch
		 * (IllegalArgumentException e) { e.printStackTrace(); } catch
		 * (FileNotFoundException e) { e.printStackTrace(); } catch (IOException
		 * e) { e.printStackTrace(); } catch (InterruptedException e) {
		 * e.printStackTrace(); }
		 */
	}

}