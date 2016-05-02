package com.bt.dataintegration.oozie.workflow.main;

import java.util.LinkedList;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.oozie.workflow.tags.ActionJava;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfigurationProperty;
import com.bt.dataintegration.oozie.workflow.tags.JavaTag;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class JavaMain implements Constants {

	private ActionJava actJava = new ActionJava();
	private JavaTag jtag = new JavaTag();
	//private GlobalConfigurationProperty gprop = new GlobalConfigurationProperty();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	private LinkedList<String> args = new LinkedList<String>();
	
	public ActionJava setJavaMain(HadoopConfig hconf) {
		
		String jobPropPath = "${nameNaode}"+
				"/user/cloudera/" + hconf.getQueueName() + "/" + 
				hconf.getTableName() + "/workspace";
		
		/*gprop.setName(JAVA_CP_QUEUENAME);
		gprop.setValue("${queueName}");*/
		jtag.setJobTracker("${jobTracker}");
		jtag.setNameNode("${nameNode}");
		//jtag.setConf(gprop);
		jtag.setMainClass(JAVA_MAIN_CLASS);
		
		args.add("${file_mask}");
		args.add("${delimiter}");
		args.add("${sourceDirectory}");
		args.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['temp_dir']}");
		args.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['rejected_dir']}");
		args.add("${control_file_name}");
		args.add("${lineNumberData}");
		args.add("${fileTrailerPresent}");
		args.add("${wf:actionData('CAPTURE_DATE_AND_CREATEDIR')['raw_dir']}");
		
		
		jtag.setArg(args);
		
		okt.setOkt("RECORD_VALIDATIONS");
		ert.setErt("CAPTURE_ERROR_LOGS_" + hconf.getHiveTableName());
		actJava.setJtag(jtag);
		actJava.setName("FILESYSTEM_VALIDATIONS");
		actJava.setOkt(okt);
		actJava.setErt(ert);
		
		return actJava;
	}
}
