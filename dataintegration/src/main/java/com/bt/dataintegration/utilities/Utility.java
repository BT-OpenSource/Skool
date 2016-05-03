package com.bt.dataintegration.utilities;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;


public class Utility {
	final static Logger logger = Logger.getLogger(Utility.class);
    public static String delimiter="";
    public static String hiveTextColumns="";
	public static int executeSSH(String cmd){
		ProcessBuilder pb = null;
		int x = 1;
		if(cmd.contains("sqoop")){
			//cmd = "sqoop import --connect jdbc:oracle:thin:@172.23.168.146:1521/XE --username HR --password hr --query \"select EMPLOYEE_ID,FIRST_NAME,LAST_NAME,EMAIL,PHONE_NUMBER,TO_CHAR(HIRE_DATE,'yyyy-mm-dd hh24:mi:ss') as HIRE_DATE,JOB_ID,SALARY,COMMISSION_PCT,MANAGER_ID,DEPARTMENT_ID from EMPLOYEES where HIRE_DATE > '01-JAN-1000' and HIRE_DATE <= TO_DATE('10-Feb-2016 3:10','dd-mm-yyyy hh24:mi') and rownum = 1 and $CONDITIONS\" --target-dir hdfs://quickstart.cloudera:8020/user/HAASDEMO_01/EMPLOYEES/landing/uncompressed_data --as-avrodatafile -m 1";
			List<String> cmdList = new LinkedList<String>();
			String[] firsthalf = cmd.split("query ");
			String[] secondhalf = firsthalf[1].split(" --target-dir ");
			String[] fh1 = firsthalf[0].split(" ");
			int z=0;
			for(String s : fh1){
				if(z == (fh1.length-1))
					cmdList.add(s+"query");
				else
					cmdList.add(s);
				z++;
			}
			z=0;
			cmdList.add(secondhalf[0]);
			cmdList.add("--target-dir");
			String[] sh1 = secondhalf[1].split(" ");
			for(String s : sh1){
				cmdList.add(s);
			}
			pb = new ProcessBuilder(cmdList).inheritIO();
		}
		else{
			String[] cmd1 = cmd.split(" ");
			pb = new ProcessBuilder(cmd1).inheritIO();
		}
		StringBuffer output = new StringBuffer();
		
		Process p;
		try {
			p = pb.start();
			x= p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return x;

	}
	
	
	public static Properties readConfigProperties(String confFileName ) {
        //System.out.println(confFileName);    
    	Properties properties = new Properties();
		try {
			
		properties.load(new FileInputStream(confFileName));
		return properties;
		
	} catch (Exception e) {
		
		System.out.println("Error loading properties");
		//e.printStackTrace();
		return null;
	}
	
}

	public static String mapDatatypes(LinkedList<String> hiveCols) {
        
        String mappedCols = "";                 
        ListIterator<String> iterator = hiveCols.listIterator();
        
        while(iterator.hasNext()) {
                       
                       String values = iterator.next();
                       String[] tokens = values.split("\\^\\^",-1);
                       String dtNew = map(tokens[1]);
                       if(tokens.length == 3){
                    	   mappedCols = mappedCols + tokens[0] + " " + dtNew + ",";
                       }
                       else{
                    	   if(tokens[3] == null || "".equalsIgnoreCase(tokens[3])){
                    		   mappedCols = mappedCols + tokens[0] + " " + dtNew + ",";
                    	   }
                    	   else{
                    		   mappedCols = mappedCols + tokens[0] + " " + dtNew + " COMMENT '"+tokens[3] +"',";
                    	   }
                       }
        }
                                      
        mappedCols = mappedCols.substring(0, mappedCols.length() - 1);
        return mappedCols;
}

private static String map(String dtOld) {
        
        String dtNew = null;
        /*String[] tokens = cols.split(" "); 
        String dtOld = tokens[1];*/
        
        switch (dtOld) {
        case "VARCHAR":
            dtNew = "STRING";
            break;
            
        case "VARCHAR2":
                       dtNew = "STRING";
                       break;

        case "NUMBER":
                       dtNew = "INT";
                       break;
                       
        case "FLOAT":
                       dtNew = "DOUBLE";
                       break;
                       
        case "LONG":
                       dtNew = "BIGINT";
                       break;
                       
        case "DATE":
                       dtNew = "TIMESTAMP";
                       break;
                       
        case "BINARY_FLOAT":
                       dtNew = "DOUBLE";
                       break;
                       
        case "BINARY_DOUBLE":
                       dtNew = "DOUBLE";
                       break;
                       
        case "TIMESTAMP":
                       dtNew = "TIMESTAMP";
                       break;
                       
        case "CHAR":
                       dtNew = "STRING";
                       break;
        
        default:
                       break;
        }
        
        return dtNew;
}

public static Map<String, String> getMappedTable(LinkedHashMap<String, List<String>> tempMap) {
	Map<String, String> hiveMappedTable = new LinkedHashMap<String, String>();

		for (Map.Entry<String, List<String>> oraTable : tempMap.entrySet()) {

			List<String> colDatatypDetail = new LinkedList<String>();
			colDatatypDetail = oraTable.getValue();
			String dataType = colDatatypDetail.get(0);
			String precision = colDatatypDetail.get(1);
			switch (dataType) {

			case "VARCHAR2":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			case "NUMBER":
				String[] precDet = {};
				try{
				precDet = precision.split(",");
				}catch(NullPointerException e){
					hiveMappedTable.put(oraTable.getKey(), "INT");
					continue;
				}
				int datTyp = Integer.parseInt(precDet[0]);
				int dec = Integer.parseInt(precDet[1]);
				if(dec>0)
				{
					if(datTyp <6)
					{
						hiveMappedTable.put(oraTable.getKey(), "FLOAT");
					}
					else
					{
						hiveMappedTable.put(oraTable.getKey(), "DOUBLE");
					}
				}
				else if(dec ==0)
				{
					if(datTyp == 1)
					{
						hiveMappedTable.put(oraTable.getKey(), "TINYINT");
					}
					else if(datTyp == 2)
					{
						hiveMappedTable.put(oraTable.getKey(), "SMALLINT");
					}
					else if(datTyp >2 && datTyp <6)
					{
						hiveMappedTable.put(oraTable.getKey(), "INT");
					}
					else if(datTyp >5)
					{
						hiveMappedTable.put(oraTable.getKey(), "BIGINT");
					}
				}
				break;
			case "FLOAT":
				hiveMappedTable.put(oraTable.getKey(), "DOUBLE");
				break;
			case "LONG":
				hiveMappedTable.put(oraTable.getKey(), "DOUBLE");
				break;
			case "DATE":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			case "BINARY_FLOAT":
				hiveMappedTable.put(oraTable.getKey(), "DOUBLE");
				break;
			case "BINARY_DOUBLE":
				hiveMappedTable.put(oraTable.getKey(), "DOUBLE");
				break;
			case "TIMESTAMP":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			case "CHAR":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			case "CLOB":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			case "BLOB":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			case "BFILE":
				hiveMappedTable.put(oraTable.getKey(), "STRING");
				break;
			default:
				break;
			}
		}


	return hiveMappedTable;
}
}
