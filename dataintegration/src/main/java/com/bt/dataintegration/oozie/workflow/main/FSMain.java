package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.oozie.workflow.tags.ActionFS;
import com.bt.dataintegration.oozie.workflow.tags.ErrorTo;
import com.bt.dataintegration.oozie.workflow.tags.FSTag;
import com.bt.dataintegration.oozie.workflow.tags.FSdelete;
import com.bt.dataintegration.oozie.workflow.tags.OkTo;

import static com.bt.dataintegration.constants.Constants.*;

public class FSMain {

	public ActionFS setFSMain() {
		
		FSTag tag = new FSTag();
		FSdelete del = new FSdelete();
		OkTo okt = new OkTo();
		ErrorTo ert = new ErrorTo();
		ActionFS act = new ActionFS();
		
		del.setPath(TEMP_FS);
		tag.setDelete(del);
		act.setName(ACTION_FS_DELETE);
		okt.setOkt(ACTION_CAPTURE_AUDIT_LOGS);
		ert.setErt(ACTION_CAPTURE_ERROR_LOGS);
		act.setOkt(okt);
		act.setErt(ert);
		act.setFsc(tag);
		
		return act;
	}
}
