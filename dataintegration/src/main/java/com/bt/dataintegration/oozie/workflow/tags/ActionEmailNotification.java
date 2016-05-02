package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"emn", "okt", "ert"})
public class ActionEmailNotification {

	private EmailNotification emn;
	private OkTo okt;
	private ErrorTo ert;
	private String name;

	public EmailNotification getEmn() {
		return emn;
	}

	@XmlElement(name="email")
	public void setEmn(EmailNotification emn) {
		this.emn = emn;
	}

	public OkTo getOkt() {
		return okt;
	}

	@XmlElement(name="ok")
	public void setOkt(OkTo okt) {
		this.okt = okt;
	}

	public ErrorTo getErt() {
		return ert;
	}

	@XmlElement(name="error")
	public void setErt(ErrorTo ert) {
		this.ert = ert;
	}

	public String getName() {
		return name;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}
}
