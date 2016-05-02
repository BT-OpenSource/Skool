package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.oozie.workflow.tags.ActionFSCreate;
import com.bt.dataintegration.oozie.workflow.tags.CreatePath;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.FSCreate;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class FSMain {

	private ActionFSCreate afsc = new ActionFSCreate();
	private FSCreate fsc = new FSCreate();
	private CreatePath cpath = new CreatePath();
	private OkTo okt = new OkTo();
	private ErrorTo ert = new ErrorTo();
	
	public ActionFSCreate setFSMain(HadoopConfig hconf) {
		
		String fsCreatePath = "${nameNode}/user/cloudera/${queueName}/${tableName}/landing";
		cpath.setPath(fsCreatePath);
		fsc.setCpath(cpath);
		
		okt.setOkt("SQOOP_IMPORT_FOR_" + hconf.getTableName());
		ert.setErt("EMAIL_FAILURE");
		
		afsc.setErt(ert);
		afsc.setOkt(okt);
		afsc.setName("CREATE_LANDING_DIR_FOR_" + hconf.getTableName());
		afsc.setFsc(fsc);
		
		return afsc;
	}
}
