package com.bt.dataintegration.fileValidations;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.EvalFunc;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pig_Validate_UDF extends EvalFunc<DataBag>{

	private static final Logger logger = LoggerFactory.getLogger(Pig_Load.class);
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private static final BagFactory mBagFactory = BagFactory.getInstance();
	private Text value;
	private String[] fieldArr;
	private String mappingDetails;
	private String[] mapFields;
	private String dateFormat;
	private String delimiter;
	private String fileTrailerKeyword;
	/*public Pig_UDF(String mappingDetails, String dateFormat, String delimiter) {
		this.mappingDetails = String.valueOf(mappingDetails);
		this.dateFormat = String.valueOf(dateFormat);
		this.delimiter = String.valueOf(delimiter);
		this.mapFields = mappingDetails.split("\\|");
		//this.threshold = Double.parseDouble(String.valueOf(threshold));
	}*/
	/*public Pig_UDF() {
		super();
	}*/

	@Override
	public DataBag  exec(Tuple input) throws IOException {
		        if (input == null || input.size() == 0)
		            return null;
		       try{
		    	   DataBag databag_final;
			    	// if valid, create a new Tuple from factory
			    	Object oBag = input.get(0);
			    	this.mappingDetails = String.valueOf(input.get(1));
					this.dateFormat = String.valueOf(input.get(2));
					this.delimiter = String.valueOf(input.get(3));
					this.fileTrailerKeyword = String.valueOf(input.get(4));
					this.mapFields = mappingDetails.split("\\|");
			    	// DataBag wrapped in a one-element Tuple
			    	if (oBag instanceof DataBag) {
				    	// @precondition check; type pig.DataBag
				    	DataBag databag = (DataBag) oBag;
				    	databag_final = parser_logic(databag);
				    	return databag_final;
				    }
		      }catch(Exception e){
		           throw new IOException("Caught exception processing input row ", e);
		     }
			return null;
	}

	
	public DataBag parser_logic(DataBag bag) throws IOException {
		int flag = 0;
		Tuple tuple = null;
		DataBag output_bag = mBagFactory.newDefaultBag();
		for (Iterator<Tuple> iterator = bag.iterator(); iterator.hasNext();) {
		try{
            tuple = iterator.next();
            String value = tuple.toString();
            if (value != null && value.length() > 0) {
            	String[] x = value.split(",");
            	/*String temp = "";
            	for(int i =1;i<x.length;i++)
            		temp = temp+x[i];*/
				fieldArr = value.split(delimiter);
					if(mapFields.length == fieldArr.length){
						for(int i=0; i<fieldArr.length; i++){
							flag = 0;
							String[] fieldValues = mapFields[i].split(",");
							if(("DATE".equalsIgnoreCase(fieldValues[1])) || ("TIMESTAMP".equalsIgnoreCase(fieldValues[1]))){
								if(("".equalsIgnoreCase(fieldArr[i])) || (" ".equals(fieldArr[i]))){
								}else{
									DateFormat originalFormat = new SimpleDateFormat(dateFormat);
								     DateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								     try{
									     Date date = originalFormat.parse(fieldArr[i]);
									     String formattedDate = targetFormat.format(date);  // 20120821
									     System.out.println(formattedDate);
									     fieldArr[i] = formattedDate;
								     }catch(Exception e){
								    	logger.error("Error parsing date", e);
								    	e.printStackTrace();
								    	flag =1;break;
								     }
								}
							}
							fieldValues = null;
						}
					}
					else{
						flag = 1;
					}
					String[] firstValues = null;
					if(flag == 1){
						firstValues = new String[]{Constants.INVALID_RECORD};
					}
					else{
						firstValues = new String[]{ Constants.VALID_RECORD};
					}
					//String[] yourArray = Arrays.copyOfRange(fieldArr, 1, fieldArr.length);
					tuple = tupleFactory.newTupleNoCopy(Arrays.asList(ArrayUtils.addAll(firstValues, fieldArr)));
					
				} 
			//}
			else{
				tuple = null;
			}
            output_bag.add(tuple);
            //return output_bag;
		}catch (Exception e) {
    			int errCode = 6018;
    			String errMsg = "Error while reading input";
    			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
    	}
		}
		return output_bag;	
	}


}
