package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="delete")
public class FSdelete {

	private String path;

	public String getPath() {
		return path;
	}

	@XmlAttribute(name="path")
	public void setPath(String path) {
		this.path = path;
	}
	
}
