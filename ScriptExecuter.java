package sample_project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import sample_project.Constants;



//import com.dbs.cst.ecl.constants.ReportGeneratorConstants;

public class ScriptExecuter extends  Constants{
	
	//This Class can be used to execute different 
	
	private  String in_path = "";
	private  String dbname = "";
	/** The Log. */
	private  Logger Log = Logger.getLogger(ScriptExecuter.class);
	/*
	 * This Class is used to excute hql scripts.
	 */
	private  String hql_script_executer = Constants.hql_script_executer;
	

	
	public  boolean shellScriptExecuter(String cmdVar) {
		Log.info("Shell Script Executer Started ");
		try {
			// String target = new String("/home/hagrawal/test.sh");
			//String hqlFileName = hqlFileNameVar.replace(".hql","");
			//String cmd = "/ECLDEV/app/scripts/shell/run_hql_test.sh " + hqlFileName;
			String target = new String(cmdVar);
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec(target);
			proc.waitFor();
			StringBuffer output = new StringBuffer();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					proc.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}
			//System.out.println("output of the Script is " + output);
			//System.out.println(line);
			return true;
		} catch (Exception e) {
			 //throw new RuntimeException(e);
			Log.error(e.getCause());
			return false;
		}
	}
	
	public  boolean hqlExecuter( SQLContext HIVECONTEXT,String hqlScript) {
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
			df.show();
			return true;
			
		}
		catch (Exception e) {
			 //throw new RuntimeException(e);
			Log.error(e.getCause());
			return false;
		}
	}
	
}
