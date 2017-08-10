package sample_project;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class TestView {

    private static final String EXCEL_FILE_LOCATION = "/tmp/EMR/Test_Cases.xls";
    
    private static DataFrame df;

    public  void automateTest (HiveContext HIVECONTEXT) {
    	
    	System.out.println("Inside automteTest");
  
        Workbook workbook = null;
        try {

            workbook = Workbook.getWorkbook(new File(EXCEL_FILE_LOCATION));

            Sheet sheet = workbook.getSheet(0);
            
            //Cell cell1 = sheet.getCell(0, 0);
            //System.out.print(cell1.getContents() + ":");    
            //Cell cell2 = sheet.getCell(0, 1);
            //System.out.println(cell2.getContents());
            
            
            //System.out.println(sheet.getRows());
            //System.out.println(sheet.getColumns());
            Cell cell2 = sheet.getCell(0,1);
            System.out.println(cell2.getContents());
            Cell cellvalue = null;
            String expectedResult = null;
            Row r1 = null;
            for(int i = 1; i < sheet.getRows(); i++){
            	for (int j = 0 ; j < sheet.getColumns(); j++) {
            		cellvalue = sheet.getCell(j, i);
            		
            		System.out.println(cellvalue.getContents());
            		/*
            		if(j == 3){
            			df = HIVECONTEXT.sql(cellvalue.getContents());
            		}
            		if(j == 5) {
            			expectedResult = cellvalue.getContents();
            		}
            		//System.out.println(cellvalue.getContents());
            		if( df.toString() == expectedResult) {
            			System.out.println("Pass");
            		}
            		*/
            		
            	}
            	
            	//cellvalue = sheet.getCell(i+1,)
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (BiffException e) {
            e.printStackTrace();
        } finally {

            if (workbook != null) {
                workbook.close();
            }

        }
    	
    	
    }

}