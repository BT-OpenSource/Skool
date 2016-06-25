package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"fsc", "okt", "ert"})
@XmlRootElement(name="action")
public class ActionFS {

	private String name;
	private FSTag fsc;
	private OkTo okt;
	private ErrorTo ert;

	public String getName() {
		return name;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	public FSTag getFsc() {
		return fsc;
	}

	@XmlElement(name="fs")
	public void setFsc(FSTag fsc) {
		this.fsc = fsc;
	}

	public OkTo getOkt() {
		return okt;
	}

	public ErrorTo getErt() {
		return ert;
	}

	@XmlElement(name="ok")
	public void setOkt(OkTo okt) {
		this.okt = okt;
	}

	@XmlElement(name="error")
	public void setErt(ErrorTo ert) {
		this.ert = ert;
	}
}
