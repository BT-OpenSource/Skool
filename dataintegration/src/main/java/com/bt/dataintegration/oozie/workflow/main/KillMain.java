package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.oozie.workflow.tags.KillTag;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class KillMain {

	private KillTag kt = new KillTag();
	
	public KillTag setKillMain() {
		
		String killMsg = "Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]";
		kt.setName("kill");
		kt.setKmsg(killMsg);
		
		return kt;
	}
}
