package com.bt.dataintegration.oozie.workflow.main;

import static com.bt.dataintegration.constants.Constants.*;

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
		
		
		/*gprop.setName(JAVA_CP_QUEUENAME);
		gprop.setValue("${queueName}");*/
		jtag.setJobTracker("${jobTracker}");
		jtag.setNameNode("${nameNode}");
		//jtag.setConf(gprop);
		jtag.setMainClass(JAVA_MAIN_CLASS);
		
		args.add("${file_mask}");
		args.add("${delimiter}");
		args.add("${sourceDirectory}");
		args.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['temp_dir']}");
		args.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['rejected_dir']}");
		args.add("${control_file_name}");
		args.add("${lineNumberData}");
		args.add("${fileTrailerPresent}");
		args.add("${wf:actionData('"+ACTION_CAPTURE_DATE_AND_CREATEDIR+"')['raw_dir']}");
		
		
		jtag.setArg(args);
		
		okt.setOkt(ACTION_RECORD_VALIDATIONS);
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		actJava.setJtag(jtag);
		actJava.setName(ACTION_FILESYSTEM_VALIDATIONS);
		actJava.setOkt(okt);
		actJava.setErt(ert);
		
		return actJava;
	}
}
