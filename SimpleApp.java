package sample_project;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
//import sample_project.testSparkFunc;



public class SimpleApp {
  public static void main(String[] args) {
	  
    String logFile = "/hivestage/ecl/dev/emr/samplelogfile.txt"; // Should be some file on your system
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile);
    testSparkFunc test1 = new testSparkFunc("warn");
    
    JavaRDD<String> numAs = logData.filter(test1);
    /*JavaRDD<String> numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("WARN"); }
    });

    JavaRDD<String> numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("warn"); }
    });
    
    */
   // JavaRDD<String> badLine = numAs.union(numBs);
    
    System.out.println("There are" +numAs.count() + "bad lines in the log file" );
    
    System.out.println("Please have a look of them");
    
    /*for (String line : badLine.foreach(f);)
    {
    	System.out.println(line);
    }*/
    
    sc.stop();
    
  }
}