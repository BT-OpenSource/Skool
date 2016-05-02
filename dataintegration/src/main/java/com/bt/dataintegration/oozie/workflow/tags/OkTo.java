package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="ok")
public class OkTo {

	private String okt;

	public String getOkt() {
		return okt;
	}

	@XmlAttribute(name="to")
	public void setOkt(String okt) {
		this.okt = okt;
	}
}
