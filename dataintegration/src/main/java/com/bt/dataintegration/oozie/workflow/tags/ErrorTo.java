package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="error")
public class ErrorTo {

	private String ert;

	public String getErt() {
		return ert;
	}

	@XmlAttribute(name="to")
	public void setErt(String ert) {
		this.ert = ert;
	}
}
