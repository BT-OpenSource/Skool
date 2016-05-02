package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.oozie.workflow.tags.EndTag;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class EndMain {

	private EndTag et = new EndTag();
	
	public EndTag setEndMain() {
		
		et.setEndName("end");
		
		return et;
	}
}
