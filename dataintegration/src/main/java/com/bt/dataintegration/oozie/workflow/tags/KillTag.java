package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;


/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="kill")
public class KillTag {

	private String kmsg;
	private String name;

	public String getKmsg() {
		return kmsg;
	}

	@XmlElement(name="message")
	public void setKmsg(String kmsg) {
		this.kmsg = kmsg;
	}

	public String getName() {
		return name;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}
}
