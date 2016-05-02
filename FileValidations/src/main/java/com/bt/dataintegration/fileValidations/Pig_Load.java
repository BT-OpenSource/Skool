package com.bt.dataintegration.fileValidations;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pig_Load extends LoadFunc{

	private static final Logger logger = LoggerFactory.getLogger(Pig_Load.class);
	//private static final Logger logger = LoadFunc.
	private RecordReader<LongWritable, Text> reader;
	private TupleFactory tupleFactory = TupleFactory.getInstance();
	private Tuple tuple = null;
	private boolean notDone;
	private Text value;
	private String[] fieldArr;
	private String mappingDetails;
	private String[] mapFields;
	private String dateFormat;
	private String delimiter;
	private String fileTrailerKeyword;
	private String fileHeaderKeyword;
	public Pig_Load(String mappingDetails, String dateFormat, String delimiter,String trailKeyword, String headKeyword) {
		this.mappingDetails = String.valueOf(mappingDetails);
		this.dateFormat = String.valueOf(dateFormat);
		this.delimiter = String.valueOf(delimiter);
		this.fileTrailerKeyword = String.valueOf(trailKeyword);
		this.fileHeaderKeyword = String.valueOf(headKeyword);
		this.mapFields = mappingDetails.split("\\|");
		//this.threshold = Double.parseDouble(String.valueOf(threshold));
	}
	
	@Override
	public InputFormat<LongWritable, Text> getInputFormat() throws IOException {
		return new TextInputFormat();

	}

	@Override
	public Tuple getNext() throws IOException {
		int flag = 0;
		//incase you have added extra columns to the mapping sheet not present in data then you can reduce the mappingLength
		// by the same number of columns
		int mappingLength = mapFields.length;
		try {
			notDone = reader.nextKeyValue();
			if (!notDone) {
				return null;
			}
			value = (Text)reader.getCurrentValue();
			if (value != null && value.getLength() > 0) {
				if(!(value.toString().contains(this.fileHeaderKeyword))){
					fieldArr = value.toString().split(delimiter,-1);
					if(mappingLength == fieldArr.length){
						for(int i=0; i<fieldArr.length; i++){
							flag = 0;
							String[] fieldValues = mapFields[i].split("\\^\\^");
							/*String isMandatory = null;
							try{
								isMandatory = fieldValues[2];
							}catch(ArrayIndexOutOfBoundsException e){
								isMandatory = "false";
							}
							if("true".equalsIgnoreCase(isMandatory)){
								if(("".equals(fieldArr[i])) || ("null".equalsIgnoreCase(fieldArr[i])) || (fieldArr[i] == null)){
									flag=1;break;
								}
							}
							if("NUMBER".equalsIgnoreCase(fieldValues[1])){
								if(("".equalsIgnoreCase(fieldArr[i])) || (" ".equals(fieldArr[i]))){
								}else{
									boolean isDecimal = fieldArr[i].matches("(.*)(\\d+)|(\\d+)(.*)(\\d+)");
									if(isDecimal == false){	
										flag=1;break;
									}
								}
							}*/
							if(("DATE".equalsIgnoreCase(fieldValues[1])) || ("TIMESTAMP".equalsIgnoreCase(fieldValues[1]))){
								if(("".equalsIgnoreCase(fieldArr[i])) || (" ".equals(fieldArr[i]))){
								}else{
									try{
									 DateFormat originalFormat = new SimpleDateFormat(dateFormat);
								     DateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								     Date date = originalFormat.parse(fieldArr[i]);
								     String formattedDate = targetFormat.format(date);  // 20120821
								     System.out.println(formattedDate);
								     fieldArr[i] = formattedDate;
								     }catch(Exception e){
								    	logger.error("Error parsing date", e);
								    	e.printStackTrace();
								    	//fieldArr[i] = "invalid_field";
								    	flag =1;break;
								     }
								}
							}
							
							//For transforming your custom field identify the index of the field.. starting from zero
							//eg.  int index = 3;
							//if(i == index){
							//fieldArr[i] = transform(fieldArr[i]);
							//}
							
							
							fieldValues = null;
						}
					}
					else{
						flag = 1;
					}
					}else{
						flag = 1;
					}
					String[] firstValues = null;
					if(flag == 1){
						firstValues = new String[]{Constants.INVALID_RECORD};
					}
					else{
						firstValues = new String[]{ Constants.VALID_RECORD};
					}
					tuple = tupleFactory.newTupleNoCopy(Arrays.asList(ArrayUtils.addAll(firstValues, fieldArr)));
					
				} 
			
			else{
				tuple = null;
			}
		} catch (InterruptedException e) {
			int errCode = 6018;
			String errMsg = "Error while reading input";
			throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
		}
		return tuple;	
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit pigSplit)
			throws IOException {
		this.reader = reader;
	//	path = ((FileSplit)pigSplit.getWrappedSplit()).getPath();
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		FileInputFormat.setInputPaths(job, location);		
	}
	//stub method
	public String transform(String value){
		String result = null;
		//your transformation logic here starts
		
		
		
		//your transformation logic here ends
		return result;
	}

}
