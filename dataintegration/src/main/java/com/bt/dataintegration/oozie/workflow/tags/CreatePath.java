package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="mkdir")
public class CreatePath {

	private String path;

	public String getPath() {
		return path;
	}

	@XmlAttribute(name="path")
	public void setPath(String path) {
		this.path = path;
	}	
}
