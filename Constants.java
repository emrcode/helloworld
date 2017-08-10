package sample_project;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

// TODO: Auto-generated Javadoc
/**
 * The Class ReportGeneratorConstants.
 */
public class Constants {

	/** The prop. */
	private static Properties prop = new Properties();
	
	/** The input. */
	private static InputStream input = null;

	/** The database name. */
	protected static String DATABASE_NAME = null;
	
	/** The report config table. */
	protected static String REPORT_CONFIG_TABLE = null;
	
	/** The report config sys table. */
	protected static String REPORT_CONFIG_SYS_TABLE = null;
	
	/** The reports save path. */
	protected static String REPORTS_SAVE_PATH = null;
	
	/** The query on particular table. */
	protected static String queryOnParticularTable = null;
	
	protected static String TB_NR_CRC_AGG = null;
	protected static String ECL9_ACCOUNTS = null;
	protected static String ECL9_BALANCES = null;
	protected static String ECL9_RATES = null;
	protected static String ECL9_GCIN = null;
	protected static String ECL9_FACILITIES = null;
	protected static String ECL9_CUSTOMERS = null;
	protected static String ECL9_MAP_BRBULE = null;
	protected static String REFTAB_CNTRY_CCY = null;
	protected static String TB_NR_PGIO_BAL_AGG = null;
	protected static String ECL9_COLLATERAL_TRADE_REP = null;
	protected static String NR_PORTF_CONTRACTS = null;
	protected static String NR_PORTF_FACILITIES = null;
	protected static String NR_PORTF_PGIO = null;
	protected static String NR_PORTF_MLC = null;
	protected static String hql_script_executer = null;
	protected static String in_path = null;

	protected static String TEMP_DIR = null;
	
	/**
	 * Load properties.
	 *
	 * @return true, if successful
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static boolean loadProperties() throws IOException {

		try {
			input = Constants.class.getClassLoader()
					.getResourceAsStream("config.properties");

			if (input != null) {
				prop.load(input);
			} else {
				throw new FileNotFoundException(
						" unable to find the properties file ");
			}
			DATABASE_NAME = prop.getProperty("database");

			REPORT_CONFIG_TABLE = prop.getProperty("conftab_downstreams");

			REPORT_CONFIG_SYS_TABLE = prop.getProperty("conftab_sys");

			REPORTS_SAVE_PATH = prop.getProperty("reports_save_path");
			hql_script_executer = prop.getProperty("hql_script_executer");

			TB_NR_CRC_AGG = prop.getProperty("tb_nr_crc_agg");
			ECL9_ACCOUNTS = prop.getProperty("ecl9_accounts");
			ECL9_BALANCES = prop.getProperty("ecl9_balances");
			ECL9_RATES = prop.getProperty("ecl9_rates");
			ECL9_GCIN = prop.getProperty("ecl9_gcin");
			ECL9_FACILITIES = prop.getProperty("ecl9_facilities");
			ECL9_CUSTOMERS = prop.getProperty("ecl9_customers");
			ECL9_MAP_BRBULE = prop.getProperty("ecl9_map_brbule");
			REFTAB_CNTRY_CCY = prop.getProperty("reftab_cntry_ccy");
			TB_NR_PGIO_BAL_AGG = prop.getProperty("tb_nr_pgio_bal_agg");
			ECL9_COLLATERAL_TRADE_REP = prop.getProperty("ecl9_collateral_trade_rep");
			NR_PORTF_CONTRACTS = prop.getProperty("nr_portf_contracts");
			NR_PORTF_FACILITIES = prop.getProperty("nr_portf_facilities");
			NR_PORTF_PGIO = prop.getProperty("nr_portf_pgio");
			NR_PORTF_MLC = prop.getProperty("nr_portf_mlc");
			in_path = prop.getProperty("in_path");
			TEMP_DIR=prop.getProperty("temp_reports_dir");
			loadQueries();

		} catch (FileNotFoundException fileNotFoundException) {
			fileNotFoundException.printStackTrace();
			return false;
		} catch (Exception exception) {
			exception.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Load queries.
	 */
	public static void loadQueries() {

		System.out.println("Queries loading");
		queryOnParticularTable = "select * from " + DATABASE_NAME;
	}
}
