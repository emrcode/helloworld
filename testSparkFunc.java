package sample_project;

import org.apache.spark.api.java.function.*;

public class testSparkFunc implements Function<String,Boolean>
{
	private String testvalue;
	
	public testSparkFunc(String value)
	{
		this.testvalue=value;
	}
	
	public Boolean call(String X)
	{
		return X.contains(testvalue);
	}
	
}