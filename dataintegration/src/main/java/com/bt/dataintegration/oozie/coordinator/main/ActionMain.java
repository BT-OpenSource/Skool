package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Action;
import com.bt.dataintegration.oozie.coordinator.tags.SLAInfo;
import com.bt.dataintegration.oozie.coordinator.tags.Workflow;
import com.bt.dataintegration.property.config.DIConfig;
import com.bt.dataintegration.property.config.HadoopConfig;

public class ActionMain {

	public Action setActionMain(Workflow workflow, SLAInfo slaInfo, HadoopConfig conf) {
		
		Action action = new Action();
		action.setWf(workflow);
		
		if(conf.isSlaRequired()) {
			action.setSlaInfo(slaInfo);
		}
		
		return action;
	}
}
