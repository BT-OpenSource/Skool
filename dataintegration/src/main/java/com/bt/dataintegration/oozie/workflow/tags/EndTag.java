package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="end")
public class EndTag {

	private String endName;

	public String getEndName() {
		return endName;
	}

	@XmlAttribute(name="name")
	public void setEndName(String endName) {
		this.endName = endName;
	}
}
