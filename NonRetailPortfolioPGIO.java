package sample_project;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.Row;

import scala.collection.Seq;
import akka.japi.function.Function;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.catalyst.expressions.Concat;

import sample_project.Constants;
import sample_project.QueryUtils;
import sample_project.ReportGenerator;

public class NonRetailPortfolioPGIO{

final static SparkConf SPARKCONF = new SparkConf().setAppName("ZRSAS_PORTFOLIO_DATA_PGIO");
	
	/*public static void main(String[] args) {
		SPARKCONF.set("spark.sql.shuffle.partitions", "3");  
		final JavaSparkContext JAVACONTEXT = new JavaSparkContext(SPARKCONF);
		final HiveContext HIVECONTEXT = new HiveContext(JAVACONTEXT.sc());
		HIVECONTEXT.setConf("spark.sql.tungsten.enabled", "false");
		QueryUtils queryUtils = new QueryUtils();
		boolean loadStatus = false;
		try {
			loadStatus = Constants.loadProperties();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(!loadStatus){
			try {
				throw new Exception(" cannot load properties ");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		DataFrame nrPortfPGIO = getPgio(HIVECONTEXT,queryUtils);
		nrPortfPGIO.write().format("com.databricks.spark.csv").option("header", "false").mode("overwrite").insertInto("nrd_app_ecl.nr_portf_pgio");
	}
	*/
	public  DataFrame getPgio(SQLContext HIVECONTEXT,QueryUtils queryUtils){
		
		//business_date extraction from conftab_sys
		String business_dat =  ReportGenerator.getBusinessDate(HIVECONTEXT, queryUtils);
	
		//extraction of ecl9_balances table
		DataFrame ecl_balance = QueryUtils.queryData(HIVECONTEXT, Constants.ECL9_BALANCES);
		DataFrame ecl_balances = ecl_balance.filter((ecl_balance.col("business_dt").equalTo(business_dat))
				.and(ecl_balance.col("acct_foreign_ccy").isNotNull()));
		
		DataFrame bal_final=ecl_balances.groupBy(ecl_balances.col("gl_account"),ecl_balances.col("cntry_cd"),ecl_balances.col("legal_entity"),
				ecl_balances.col("business_unit"),ecl_balances.col("acct_foreign_ccy"),ecl_balances.col("gl_product_cd"),ecl_balances.col("pc_code"))
				.agg(functions.sum(ecl_balances.col("acct_foreign_amt")).as("acct_foreign_amt"));
	    
		/*DataFrame bal_final=ecl_balances.groupBy(ecl_balances.col("gl_account")).agg(functions.sum(ecl_balances.col("acct_foreign_amt")).as("acct_foreign_amt"))
				.select(ecl_balances.col("cntry_cd").as("cntry_cd"),ecl_balances.col("legal_entity").as("legal_entity"),
						ecl_balances.col("business_unit").as("business_unit"),ecl_balances.col("acct_foreign_ccy").as("acct_foreign_ccy"),
						ecl_balances.col("acct_foreign_amt").as("acct_foreign_amt"),
						ecl_balances.col("gl_product_cd").as("gl_product_cd"),ecl_balances.col("pc_code").as("pc_code"));*/
		
	/*	DataFrame grp_bal=HIVECONTEXT.sql("select sum(acct_foreign_amt) as acct_foreign_amt,gl_account as gl_account from df_balances group by gl_account");*/	
					
		//extraction of REFTAB_CNTRY_CCY table
		DataFrame reftab = QueryUtils.queryData(HIVECONTEXT, Constants.REFTAB_CNTRY_CCY);
		
		//joining ecl9_balances and REFTAB_CNTRY_CCY
		DataFrame bal_reftab=bal_final.join(reftab,reftab.col("src_cntry_cd").equalTo(bal_final.col("cntry_cd")),"inner");
		
		//aggregate table pgio_bal_agg
		DataFrame pgioBalAgg = bal_reftab.select(lit(business_dat).as("business_dt"),
				concat(lit("PGIO"),lit("_"),bal_reftab.col("legal_entity"),lit("_"),
						bal_reftab.col("business_unit"),lit("_"),bal_reftab.col("gl_account"),lit("_"),bal_reftab.col("gl_product_cd"),lit("_")
						,bal_reftab.col("pc_code"),lit("_"),bal_reftab.col("acct_foreign_ccy"),lit("_"),lit("PGIO")).as("contract_reference"),		
				bal_reftab.col("legal_entity").as("legal_entity"),bal_reftab.col("business_unit").as("business_unit"),
				bal_reftab.col("cntry_cd").as("cntry_cd"),bal_reftab.col("acct_foreign_ccy").as("acct_foreign_ccy"),
				bal_reftab.col("tgt_local_ccy").as("acct_local_ccy"),bal_reftab.col("tgt_base_ccy").as("acct_base_ccy")
				,bal_reftab.col("acct_foreign_amt").as("acct_foreign_amt_agg"),
				bal_reftab.col("gl_account").as("gl_account"),bal_reftab.col("gl_product_cd").as("gl_product_cd"),bal_reftab.col("pc_code").as("pc_code"));
		

				//extraction of data from tb_nr_crc_agg table
				DataFrame tb_nr_cr_agg = QueryUtils.queryData(HIVECONTEXT, Constants.TB_NR_CRC_AGG);
				DataFrame tb_nr_crc_agg = QueryUtils.getSelectedColumns(HIVECONTEXT, "contract_reference,pd_original,ead,maturity_date,ccf,seniority,counterparty_id", tb_nr_cr_agg);
					
				//extraction of data from ecl9_rates table
				DataFrame ecl_rate=QueryUtils.queryData(HIVECONTEXT, Constants.ECL9_RATES);
				DataFrame ecl_rates=QueryUtils.getSelectedColumns(HIVECONTEXT, "from_ccy,to_ccy,rate,rate_type", ecl_rate);
				DataFrame rates_rename = (ecl_rates.withColumnRenamed("rate","ratess").withColumnRenamed("from_ccy","from_ccys")
						.withColumnRenamed("to_ccy", "to_ccys").withColumnRenamed("rate_type", "rate_types"));
				
				//extraction of data from ecl9_gcin table
				DataFrame ecl_gcn=QueryUtils.queryData(HIVECONTEXT, Constants.ECL9_GCIN);
				DataFrame ecl_gcin=QueryUtils.getSelectedColumns(HIVECONTEXT, "gcin,cust_full_name,cntry_of_domicile,masic_cd,cust_acrr,"
						+ "rating_model,cust_accr_dt,cntry_of_risk,cust_type,cust_owner,cust_segment,cust_rm,dbsic_cd,cntry_of_incorp,business_dt", ecl_gcn);
						
				//joining tb_nr_crc_agg and pgio_bal_agg
				DataFrame crc_pgioBalAgg=tb_nr_crc_agg.join(pgioBalAgg,pgioBalAgg.col("contract_reference").equalTo(tb_nr_crc_agg.col("contract_reference")),"inner")
						.drop(pgioBalAgg.col("contract_reference"));
				
				
				//joining pgio_bal_agg,ecl9_balances with gcin 
				DataFrame crc_pgio_gcin=crc_pgioBalAgg.join(ecl_gcin,crc_pgioBalAgg.col("counterparty_id").equalTo(ecl_gcin.col("gcin")),"left")
						.drop(ecl_gcin.col("business_dt"));
				
				//joining resultant with rates table				
				DataFrame crc_pgio_gcin_rate=crc_pgio_gcin.join(ecl_rates,(ecl_rates.col("from_ccy").equalTo(crc_pgio_gcin.col("acct_foreign_ccy"))
						.and(ecl_rates.col("rate_type").equalTo(lit("STANDARD")))),"left");
			
				//exrate_to_base_ccy and exrate_to_local_ccy and exrate_to_sgd columns 
				DataFrame rates_base=crc_pgio_gcin_rate.withColumn("exrate_to_base_ccy",functions.when(crc_pgio_gcin_rate.col("to_ccy").equalTo(crc_pgio_gcin_rate.col("acct_base_ccy"))
								.and(((crc_pgio_gcin_rate.col("rate")).isNotNull()).and(crc_pgio_gcin_rate.col("rate").geq(0))),crc_pgio_gcin_rate.col("rate")).otherwise(functions.lit("1")))
						.withColumn("exrate_to_local_ccy",functions.when(crc_pgio_gcin_rate.col("to_ccy").equalTo(crc_pgio_gcin_rate.col("acct_local_ccy"))
								.and(((crc_pgio_gcin_rate.col("rate")).isNotNull()).and(crc_pgio_gcin_rate.col("rate").geq(0))),crc_pgio_gcin_rate.col("rate")).otherwise(functions.lit("1")))
						.withColumn("exrate_to_sgd",functions.when(crc_pgio_gcin_rate.col("to_ccy").equalTo(lit("SGD"))
								.and(((crc_pgio_gcin_rate.col("rate")).isNotNull()).and(crc_pgio_gcin_rate.col("rate").geq(0))),crc_pgio_gcin_rate.col("rate")).otherwise(functions.lit("1")))
								.where(crc_pgio_gcin_rate.col("to_ccy").equalTo(crc_pgio_gcin_rate.col("acct_base_ccy"))
										.or(crc_pgio_gcin_rate.col("to_ccy").equalTo(crc_pgio_gcin_rate.col("acct_local_ccy")))
										.or(crc_pgio_gcin_rate.col("to_ccy").equalTo(lit("SGD"))));
				
				//joining resultant with RATE		
				DataFrame final_df=rates_base.join(rates_rename,(rates_rename.col("from_ccys").equalTo(lit("SGD"))
						.and(rates_rename.col("rate_types").equalTo(lit("STANDARD"))).and(rates_rename.col("to_ccys")
								.equalTo(rates_base.col("acct_foreign_ccy")))),"left");
				DataFrame main_df=final_df.withColumn("limit",final_df.col("ratess").multiply(final_df.col("ead")));
				
			
			//inserting data to nr_portf_pgio	
				DataFrame nrPortfPGIO=main_df.select(lit("D").as("rec_type"),
						lit(business_dat).as("strike_date"),
						main_df.col("contract_reference").as("transaction_id"),
						lit("").as("facility_id"),
						lit("").as("facility_product"),
						(main_df.col("gcin"))
								.as("counterparty_id"),
						(main_df.col("cust_full_name"))
								.as("counterparty_name"),
								(functions.when((((main_df.col("cntry_of_domicile")).isNotNull())
										.and(trim(main_df.col("cntry_of_domicile"))
												.notEqual(lit("")))),
								main_df.col("cntry_of_domicile")).when(
								(((main_df.col("cntry_of_incorp")).isNotNull())
										.and(trim(main_df.col("cntry_of_incorp"))
												.notEqual(lit("")))),
								main_df.col("cntry_of_incorp")).otherwise("SG")).as("country_code"),
						main_df.col("masic_cd").as("industry_code"),
						main_df.col("pd_original").as("pd_calculated"),
						main_df.col("pd_original").as("pd_final"),
						main_df.col("cust_acrr").as("grade_final"),
						main_df.col("pd_original").as("lgd_ttc"),
						main_df.col("limit").as("limit"),
						main_df.col("acct_foreign_ccy").as("limit_ccy"),
						functions.to_date(unix_timestamp(main_df.col("maturity_date"),"yyyymmdd").cast("timestamp")).as("maturity"),					
						main_df.col("limit").as("ead"),
						main_df.col("acct_foreign_ccy").as("ead_ccy"),				
						main_df.col("ccf").as("ccf"),
						lit("").as("fcf"),
						main_df.col("rating_model").as("pd_model_ver"),
						lit("GEN2").as("lgd_model_ver"),
						lit("REGCAP").as("ead_model_ver"),
						main_df.col("acct_foreign_amt_agg").as("drawn"),
						main_df.col("acct_foreign_ccy").as("drawn_ccy"),
						lit("").as("eir"),
						lit("").as("repayment_type"),
						lit("").as(
								"facility_origination_date"),
						main_df.col("cust_accr_dt").as("last_grading_date"),
						lit("").as("pd_origination_date"), 
						lit("").as("origination_pd"),
						main_df.col("seniority").as("seniority"),
						lit("").as("committed"),
						main_df.col("legal_entity").as("legal_entity_cde"), 
						lit("").as("branch_cde"),
						main_df.col("cntry_cd").as("country"),
						main_df.col("counterparty_id").as("lcin"), 
						main_df.col("gcin")
								.as("gcin"), main_df.col("cust_type").as("cust_type"),
						main_df.col("cntry_of_risk").as("country_of_risk"), main_df
								.col("cntry_of_incorp").as("country_of_incorp"),
						main_df.col("cust_owner").as("cust_owner"),
						main_df.col("cust_segment").as("cust_segment"),
						lit("").as("cust_subsegment"), main_df
								.col("dbsic_cd").as("dbsic"),
								lit("").as("dbsic_desc"), main_df.col("masic_cd").as("masic"),
						lit("").as("masic_desc"),
						main_df.col("rating_model").as("rating_model"),
						lit("").as("rating_approver"),
						main_df.col("cust_rm").as("rel_mangr"), lit("").as("ibg_seg"),
						functions.concat(main_df.col("gl_account"),lit("-"),main_df.col("gl_product_cd")).as("acct_number"),
						lit("PGIO").as("src_sys_cde"),
						lit("").as("src_prod_cde"), lit("").as("src_prod_subcde"),
						main_df.col("acct_base_ccy").as("base_ccy"),
						main_df.col("acct_local_ccy").as("local_ccy"),
						lit("").as("days_past_due"), lit("").as("npl_flag"), lit("")
								.as("watchlist_category"),
						lit("").as("watchlistcategory_date"),
						main_df.col("exrate_to_base_ccy").as("exrate_to_base_ccy"),
						main_df.col("exrate_to_local_ccy").as("exrate_to_local_ccy"),
						main_df.col("exrate_to_sgd").as("exrate_to_sgd"),
						main_df.col("business_unit").as("business_unit"),
						main_df.col("gl_account").as("gl_product"), main_df.col("gl_product_cd").as("gl_account"),
						lit("").as("original_limit_amt"),
						lit("").as("revolving_flag"),
						lit("").as("approlval_date"),
						lit("").as("drawdown_date"), lit("")
								.as("dpd_ind"), lit("").as("months_on_book"), lit("")
								.as("residual_maturity"),
						lit("").as("original_maturity"), lit("").as("pd_pool_id"),
						lit("").as("lgd_pool_id"), lit("").as("rwa"), lit("").as("el"),
						lit("").as("basel_asset_class"), lit("").as("basel_approach"),
						lit("").as("product_type_grp"),
						lit("").as("broad_industry_sector"),
						lit("").as("sub_industry_sector"),
						lit("").as("tranche_collateral_type"),
						lit("").as("basel_ass_group"),
						functions.to_date(unix_timestamp(main_df.col("maturity_date"),"yyyymmdd").cast("timestamp")).as("md_orig_maturity_dte"),
						main_df.col("pd_original").as("md_pd_final"),
						main_df.col("contract_reference").as("src_contract_reference"),
						lit("W").as("wholesale_retail_ind"));
				
			
			
			nrPortfPGIO.show();	
		
			return(nrPortfPGIO);
	
		
	}

	
	
	
}