package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="start")
public class StartTo {

	private String startTo;

	public String getStartTo() {
		return startTo;
	}

	@XmlAttribute(name="to")
	public void setStartTo(String startTo) {
		this.startTo = startTo;
	}
}
