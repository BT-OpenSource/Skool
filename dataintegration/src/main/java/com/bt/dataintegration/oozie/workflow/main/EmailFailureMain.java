package com.bt.dataintegration.oozie.workflow.main;

import static com.bt.dataintegration.constants.Constants.*;
import com.bt.dataintegration.oozie.workflow.tags.ActionEmailNotification;
import com.bt.dataintegration.oozie.workflow.tags.EmailNotification;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class EmailFailureMain{

	private EmailNotification notifyFailure = new EmailNotification();
	private ActionEmailNotification actFailure = new ActionEmailNotification();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	
	public ActionEmailNotification setEmailSuccessMain() {
		
		String emailSubject = "FAILURE: ${wf:name()} : ${wf:id()}";
		String emailBody = "Hi,\n\n${wf:id()} : ${wf:name()} failed !!!\nPlease check job-tracker logs for more detail.\n\n"
					+ "This is an auto-generated email.\nPlease do not reply to this email.\n\n"
					+ "Thanks,\nDSS\nBig Data Team.";
		
		notifyFailure.setXmlns(EMAIL_XMLNS);
		notifyFailure.setBody(emailBody);
		notifyFailure.setEmailTo("${failure_emails}");
		notifyFailure.setSubject(emailSubject);
		
		okt.setOkt("kill");
		ert.setErt("kill");
		
		actFailure.setName(ACTION_EMAIL_FAILURE);
		actFailure.setEmn(notifyFailure);
		actFailure.setOkt(okt);
		actFailure.setErt(ert);
		
		return actFailure;
	}
}
