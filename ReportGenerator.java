package sample_project;

import java.io.IOException;
import java.net.URI;import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;


import sample_project.Constants;
import sample_project.IFRSCustomException;
import sample_project.QueryUtils;

/**
 * The Class ReportGenerator.
 */
public class ReportGenerator extends Constants {

	/** The Log. */
	private static Logger Log = Logger.getLogger(ReportGenerator.class);

	/** The report attribs. */
	static HashMap<String, Object> reportAttribs = new HashMap<String, Object>();

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void main(String[] args) throws IOException {
		try {
			SparkConf conf = new SparkConf().setAppName("IFRS");
			JavaSparkContext jsc = new JavaSparkContext(conf);
			SQLContext hc = new HiveContext(jsc.sc());
			FileSystem fs = FileSystem.get(jsc.hadoopConfiguration());
			// String id = "ZRSAS_RATES";

			String id = args[0].toString();
			boolean loadPropertiesStatus = Constants.loadProperties();

			if (!loadPropertiesStatus) {
				jsc.close();
				throw new IFRSCustomException(
						" unable to load properties (config.properties) from the path : /src/main/resources/config.properties ");
			}
			QueryUtils queryUtils = new QueryUtils();
			reportAttribs = generateReport(hc, id, queryUtils);
			DataFrame dataFromDataSrc = (DataFrame) reportAttribs
					.get("dataFromSourceTable");
			String delimiter = (String) reportAttribs.get("delimiter");
			String fileName = (String) reportAttribs.get("fileName");
			//fileName = "ZRSAS_FOREX_RATES_20170623.dat";
			String reportsSavepath = Constants.REPORTS_SAVE_PATH;
			//String tempFolder = Constants.temp_reports_dir;
			String tempFolder = "";
			if (reportsSavepath == null || reportsSavepath.isEmpty()) {
				jsc.close();
				throw new IFRSCustomException(
						" Cannot find the path for saving the generated reports");
			}

			String headerIndicator = (String) reportAttribs
					.get("headerIndicator");
			String header = (String) reportAttribs.get("header");

			String trailerIndicator = (String) reportAttribs
					.get("trailerIndicator");
			String trailer = (String) reportAttribs.get("trailer");
			String colHeader = (String) reportAttribs.get("colHeader");
			String includeColumnHeaders = "false";
			if (colHeader.equals("y") || colHeader.equalsIgnoreCase("Y")) {
				includeColumnHeaders = "true";
			}
			dataFromDataSrc.coalesce(1).write()
					.format("com.databricks.spark.csv")
					.mode(SaveMode.Overwrite)
					.option("header", includeColumnHeaders)
					.option("delimiter", delimiter)
					.save(reportsSavepath + fileName);

			
			
			/*reportsSavepath="/hivestage/ecl/dev/out/tempReports/";
			Runtime.getRuntime().exec("hadoop fs -mkdir "+ reportsSavepath);
			*/
			fs.rename(new Path(reportsSavepath + fileName + "/part-00000"),
					new Path(reportsSavepath + "/" + fileName + "-output"));
			fs.delete(new Path(reportsSavepath + fileName + "/"), true);
			fs.rename(new Path(reportsSavepath + "/" + fileName + "-output"),
					new Path(reportsSavepath + "/" + fileName));
			String sourceFile = reportsSavepath + "/" + fileName;
			
			
			if (headerIndicator.equalsIgnoreCase("y")
					|| trailerIndicator.equalsIgnoreCase("y")) {
				String headerText = null, trailerText = null;
				String date = (String) reportAttribs.get("formattedDate");
				if (headerIndicator.equalsIgnoreCase("y")) {
					date = getDateForHeader(hc, queryUtils, header);
					headerText = header.replaceAll("YYYYMMDD", date);
				}
				if (trailerIndicator.equalsIgnoreCase("y"))
					trailerText = trailer.replaceAll("\\[(.*?)\\]", String
							.valueOf(dataFromDataSrc.collectAsList().size()));

				if (fs.exists(new Path(tempFolder))) {
					Runtime.getRuntime().exec(" hdfs dfs -rmr " + 	tempFolder);
				}
				Runtime.getRuntime().exec(" hdfs dfs -mkdir "+ tempFolder);
				
				Runtime.getRuntime().exec(" hdfs dfs -touchz "+tempFolder+"/"+"header.txt");
				
				String headerPath = tempFolder+"/"+"header.txt";
				
				FSDataOutputStream fileOutputStream = null;
				FSDataOutputStream out = null;

				out = fs.create(new Path(headerPath));
				out.write(headerText.getBytes());				
				out.write("\n".getBytes());

				FSDataInputStream in = null;

				in = fs.open(new Path(sourceFile));
				IOUtils.copyBytes(in, out, 4096, false);

				/*
				 * fileOutputStream = fs.append(new Path( headerPath));
				 */
				out.writeBytes(trailerText);
				fs.rename(new Path(headerPath),new Path(tempFolder+"/"+fileName));
				fs.delete(new Path(reportsSavepath + "/" + fileName),true);
				fs.rename(new Path(tempFolder+"/"+fileName),new Path(reportsSavepath + "/" + fileName));
			//	fs.delete(new Path("/hivestage/ecl/dev/out/temp/"),true);
				//Runtime.getRuntime().exec("hdfs dfs -rmr /hivestage/ecl/dev/out/temp/");
				System.out.println(" final step ");
				System.out.println("hdfs dfs -rmr "+ tempFolder);
				fs.delete(new Path(tempFolder),true);
				//Runtime.getRuntime().exec(" hdfs dfs -rmr "+ tempFolder);
				
			}
			jsc.close();
		} catch (IFRSCustomException ifrsCustomException) {
			ifrsCustomException.printStackTrace();
		} catch (Exception exception) {
			exception.printStackTrace();

		}
	}

	/**
	 * Generate report.
	 * 
	 * @param hc
	 *            the hc
	 * @param id
	 *            the id
	 * @param queryUtils
	 *            the query utils
	 * @return the hash map
	 */
	public static HashMap<String, Object> generateReport(SQLContext hc,
			String id, QueryUtils queryUtils) {

		DataFrame reportAttributesCols = null;

		try {
			if (hc != null) {

				System.out.println(" table name :"
						+ Constants.REPORT_CONFIG_TABLE);

				reportAttributesCols = queryUtils.queryData(hc,
						Constants.REPORT_CONFIG_TABLE);
				System.out.println("report 1:");

				reportAttributesCols.show();

				DataFrame reportAttributes = reportAttributesCols.select(
						reportAttributesCols.col("filename"),
						reportAttributesCols.col("delimiter"),
						reportAttributesCols.col("dev_conn"),
						reportAttributesCols.col("data_src"),
						reportAttributesCols.col("hdr_ind"),
						reportAttributesCols.col("hdr"),
						reportAttributesCols.col("trl_ind"),
						reportAttributesCols.col("trl"),
						reportAttributesCols.col("col_hdr"),
						reportAttributesCols.col("id")).where(
						reportAttributesCols.col("id").equalTo(id));

				reportAttributes.show();

				List<Row> columnNames = extractFields(reportAttributes);
				System.out.println(" column Names " + columnNames);
				String fileName = columnNames.get(0).get(0).toString();
				String delimiter = columnNames.get(0).get(1).toString();
				String dataSourceTable = columnNames.get(0).get(3).toString();
				String headerIndicator = columnNames.get(0).get(4).toString();
				String header = columnNames.get(0).get(5).toString();

				String trailerIndicator = columnNames.get(0).get(6).toString();
				String trailer = columnNames.get(0).get(7).toString();

				String colHeader = columnNames.get(0).get(8).toString();
				DataFrame dataFromSourceTable = queryUtils.queryData(hc,
						dataSourceTable);
				System.out.println("filename  before regex " + fileName);
				String s = fileName;
				s = s.replaceAll("\\[(.*?)\\]", "");
				System.out.println(s);
				String[] fileNameSplit = s.split("\\.");
				String date = getDate(hc, queryUtils);
				String formattedDate = date.replaceAll("-", "");
				fileName = fileNameSplit[0].concat(formattedDate).concat(".")
						.concat(fileNameSplit[1]);
				System.out.println("file name after regex :  " + fileName);
				reportAttribs.put("dataFromSourceTable", dataFromSourceTable);
				reportAttribs.put("delimiter", delimiter);
				reportAttribs.put("fileName", fileName);
				reportAttribs.put("headerIndicator", headerIndicator);
				reportAttribs.put("header", header);
				reportAttribs.put("trailerIndicator", trailerIndicator);
				reportAttribs.put("trailer", trailer);
				reportAttribs.put("colHeader", colHeader);
				reportAttribs.put("formattedDate", formattedDate);

			}

		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return reportAttribs;
	}

	/**
	 * Extract fields.
	 * 
	 * @param dataFrame
	 *            the data frame
	 * @return the list
	 */
	public static List<Row> extractFields(DataFrame dataFrame) {
		return dataFrame.collectAsList();
	}

	/**
	 * Gets the business date.
	 * 
	 * @param hc
	 *            the hc
	 * @param queryUtils
	 *            the query utils
	 * @return the business date
	 */
	public static String getBusinessDate(SQLContext hc, QueryUtils queryUtils) {
		DataFrame businessDateDF = queryUtils.queryData(hc,
				Constants.REPORT_CONFIG_SYS_TABLE);
		return extractFields(businessDateDF).get(0).get(0).toString();
	}

	public static String getBatchDate(SQLContext hc, QueryUtils queryUtils) {
		DataFrame businessDateDF = queryUtils.queryData(hc,
				Constants.REPORT_CONFIG_SYS_TABLE);
		return extractFields(businessDateDF).get(0).get(1).toString();
	}

	public static String getBatchStatus(SQLContext hc, QueryUtils queryUtils) {
		DataFrame businessDateDF = queryUtils.queryData(hc,
				Constants.REPORT_CONFIG_SYS_TABLE);
		return extractFields(businessDateDF).get(0).get(2).toString();
	}
	
	

	public static String getDateForHeader(SQLContext hc, QueryUtils queryUtils, String header){
		String date=null;
		if (header.contains("business")){
			System.out.println(" In business date");
			date = getBusinessDate(hc, queryUtils).replaceAll("-",
					"");
		}
		if (header.contains("batch_dt")){
			
			System.out.println(" In batch date ");
			date = getBatchDate(hc, queryUtils).replaceAll("-", "as");
		}
		if (header.contains("batch_status"))
			date = getBatchDate(hc, queryUtils).replaceAll("-", "");
		
		return date;
	}
	
	public static String  getDate(SQLContext hc, QueryUtils queryUtils){
		DataFrame businessDateDF = queryUtils.queryData(hc,
				Constants.REPORT_CONFIG_TABLE);
		String date = extractFields(businessDateDF).get(0).get(2).toString();
		return getDateForHeader(hc, queryUtils, date);
	}
	
}
