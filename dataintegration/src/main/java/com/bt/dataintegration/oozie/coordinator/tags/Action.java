package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="action")
public class Action {

	private Workflow wf;

	public Workflow getWf() {
		return wf;
	}

	@XmlElement(name="workflow")
	public void setWf(Workflow wf) {
		this.wf = wf;
	}

}
