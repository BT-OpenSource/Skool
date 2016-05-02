package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"name", "cred", "hs2Tag", "okt", "ert"})
@XmlRootElement(name="action")
public class ActionHive2 {

	private Hive2Tag hs2Tag;
	private OkTo okt;
	private ErrorTo ert;
	private String name;
	private String cred;

	public Hive2Tag getHs2Tag() {
		return hs2Tag;
	}

	@XmlElement(name="hive2")
	public void setHs2Tag(Hive2Tag hs2Tag) {
		this.hs2Tag = hs2Tag;
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

	public String getCred() {
		return cred;
	}

	@XmlAttribute(name="cred")
	public void setCred(String cred) {
		this.cred = cred;
	}
}
