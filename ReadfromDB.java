package sample_project;
/*
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class ReadfromDB implements Serializable {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(ReadfromDB.class);

    private static final String MYSQL_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String MYSQL_USERNAME = "eclapp";
    private static final String MYSQL_PWD = "ZAQ!2wsxcde3";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mariadb://10.92.154.141:6603/ecldev?user=" + MYSQL_USERNAME + "&password=" + MYSQL_PWD;

    private static final JavaSparkContext sc =
            new JavaSparkContext(new SparkConf().setAppName("SparkJdbcDs").setMaster("local[*]"));

    private static final SQLContext sqlContext = new SQLContext(sc);

    public static void main(String[] args) {
        //Data source options
        Map<String, String> options = new HashMap<>();
        options.put("driver", MYSQL_DRIVER);
        options.put("url", MYSQL_CONNECTION_URL);
        options.put("dbtable",
                    "(select * from sample) as sample");
        //options.put("partitionColumn", "emp_no");
        //options.put("lowerBound", "10001");
        //options.put("upperBound", "499999");
        //options.put("numPartitions", "10");

        //Load MySQL query result as DataFrame
        DataFrame jdbcDF = sqlContext.load("jdbc", options);

        List<Row> employeeFullNameRows = jdbcDF.collectAsList();

        for (Row employeeFullNameRow : employeeFullNameRows) {
            LOGGER.info(employeeFullNameRow);
            System.out.println(employeeFullNameRow);
        }
        
        
    }
}
*/
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class ReadfromDB {

                public static void main(String[] args) throws ParseException, FileNotFoundException, IOException {
                
                                SparkConf conf = new SparkConf().setAppName("IFRS");
                                JavaSparkContext jsc = new JavaSparkContext(conf);
                                SQLContext hc =  new HiveContext(jsc.sc());
                                
        final String mySqlConnectionUrl ="jdbc:mysql://10.92.154.141:6603/ecldev" + "?user="
                +"eclapp" + "&password=" + "ZAQ!2wsxcde3";

        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", "com.mysql.jdbc.Driver");
        options.put("url",mySqlConnectionUrl);
        options.put("dbtable", "sample");
                                System.out.println("***********************");
                                DataFrame temp1 =  hc.read().format("jdbc").options(options).load();
                                temp1.show();
                                }
}
