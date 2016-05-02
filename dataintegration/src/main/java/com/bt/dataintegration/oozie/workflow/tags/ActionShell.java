package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = { "sTag", "okt", "ert" })
@XmlRootElement(name = "action")
public class ActionShell {

	private ShellTag sTag;
	private OkTo okt;
	private ErrorTo ert;
	private String name;

	public ShellTag getsTag() {
		return sTag;
	}

	public OkTo getOkt() {
		return okt;
	}

	public ErrorTo getErt() {
		return ert;
	}

	@XmlElement(name="shell")
	public void setsTag(ShellTag sTag) {
		this.sTag = sTag;
	}

	@XmlElement(name="ok")
	public void setOkt(OkTo okt) {
		this.okt = okt;
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
