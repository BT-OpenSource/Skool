package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = {"wf", "slaInfo"})
@XmlRootElement(name="action")
public class Action {

	private Workflow wf;
	private SLAInfo slaInfo;
	
	public SLAInfo getSlaInfo() {
		return slaInfo;
	}

	@XmlElement(name="sla:info")
	public void setSlaInfo(SLAInfo slaInfo) {
		this.slaInfo = slaInfo;
	}

	public Workflow getWf() {
		return wf;
	}

	@XmlElement(name="workflow")
	public void setWf(Workflow wf) {
		this.wf = wf;
	}

}
