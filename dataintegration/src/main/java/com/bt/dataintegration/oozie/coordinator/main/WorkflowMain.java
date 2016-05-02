package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Workflow;

public class WorkflowMain {

	public Workflow setWorkflowMain() {
		
		Workflow workflow = new Workflow();
		workflow.setAppPath("${wf_application_path}");
		return workflow;
	}
}
