package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="workflow")
public class Workflow {

	private String appPath;

	public String getAppPath() {
		return appPath;
	}

	@XmlElement(name="app-path")
	public void setAppPath(String appPath) {
		this.appPath = appPath;
	}

}
