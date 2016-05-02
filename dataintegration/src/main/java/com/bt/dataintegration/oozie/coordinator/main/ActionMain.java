package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Action;
import com.bt.dataintegration.oozie.coordinator.tags.Workflow;

public class ActionMain {

	public Action setActionMain(Workflow workflow) {
		
		Action action = new Action();
		action.setWf(workflow);
		return action;
	}
}
