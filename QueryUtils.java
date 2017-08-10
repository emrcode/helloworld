package sample_project;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

// TODO: Auto-generated Javadoc
/**
 * The Class QueryUtils.
 */
public class QueryUtils extends Constants {

	/**
	 * Query data.
	 *
	 * @param hcContext the hc context
	 * @param tableName the table name
	 * @return the data frame
	 */
	public static DataFrame queryData(SQLContext hcContext, String tableName) {

		String query = Constants.queryOnParticularTable;
		
		System.out.println("query: " + Constants.queryOnParticularTable.concat(".").concat(tableName));
		try {
			if (query != null)
				query = Constants.queryOnParticularTable
				.concat(".").concat(tableName);
				if(query.endsWith(".")){
					throw new IFRSCustomException
					("Table/Database might be missing or table/Database not found. Table name is " 
				+ tableName+"database name is "+ Constants.DATABASE_NAME );
				}
				return hcContext
						.sql(Constants.queryOnParticularTable
								.concat(".").concat(tableName));
				
		} 
		catch(IFRSCustomException ifrsCustomException){
			ifrsCustomException.getMessage();
			ifrsCustomException.printStackTrace();
			
		}catch (Exception exception) {
			exception.printStackTrace();
		}
		return hcContext
				.sql(Constants.queryOnParticularTable
						.concat(".").concat(tableName));
	
	
	}
	public DataFrame getData(SQLContext hcContext, String query) {

		
		try {
			if (query != null)
				return hcContext
						.sql(query);
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return null;
	}
	
	public static DataFrame getSelectedColumns(SQLContext hcContext , String columns,DataFrame dataframe){

		return dataframe.selectExpr(columns.split(","));
	

	} 

	
}
