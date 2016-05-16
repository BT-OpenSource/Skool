package com.bt.dataintegration.oozie.workflow.main;

import static com.bt.dataintegration.constants.Constants.*;
import com.bt.dataintegration.oozie.workflow.tags.ActionEmailNotification;
import com.bt.dataintegration.oozie.workflow.tags.EmailNotification;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class EmailSuccessMain {

	private EmailNotification notifySuccess = new EmailNotification();
	private ActionEmailNotification actSuccess = new ActionEmailNotification();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	
	public ActionEmailNotification setEmailSuccessMain(HadoopConfig conf) {
		String sqoopAction = ACTION_SQOOP_IMPORT;
		
		String emailSubject = "SUCCESS: ${wf:name()} : ${wf:id()}";
		String emailBody = "";
		if(conf.getImport_export_flag().equals(FILE_IMPORT)){
		emailBody = "Hi,\n\n${wf:id()} : ${wf:name()} completed successfully !\n"
				+ "Number of Records Processed = ${hadoop:counters(\""+ACTION_RECORD_VALIDATIONS+"\")[\"RECORD_WRITTEN\"]}\n\n"
					+ "This is an auto-generated email.\nPlease do not reply to this email.\n\n"
					+ "Thanks,\nDSS\nBig Data Team.";
		}
		else if(conf.getImport_export_flag().equals(SQOOP_IMPORT)){
		emailBody = "Hi,\n\n${wf:id()} : ${wf:name()} completed successfully !\n"
				+ "Number of Records Fetched = ${hadoop:counters(\""+sqoopAction+"\")[\"org.apache.hadoop.mapred.Task$Counter\"][\"MAP_OUTPUT_RECORDS\"]}\n\n"
					+ "This is an auto-generated email.\nPlease do not reply to this email.\n\n"
					+ "Thanks,\nDSS\nBig Data Team.";
		}
		notifySuccess.setXmlns(EMAIL_XMLNS);
		notifySuccess.setBody(emailBody);
		notifySuccess.setEmailTo("${success_emails}");
		notifySuccess.setSubject(emailSubject);
		
		okt.setOkt("end");
		ert.setErt("kill");
		
		actSuccess.setName(ACTION_EMAIL_SUCCESS);
		actSuccess.setEmn(notifySuccess);
		actSuccess.setOkt(okt);
		actSuccess.setErt(ert);
		
		return actSuccess;
	}
}
