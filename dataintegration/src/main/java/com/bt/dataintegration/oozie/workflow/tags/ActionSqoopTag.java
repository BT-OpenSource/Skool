package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"sqTag", "okTo", "errorTo"})
@XmlRootElement(name="action")
public class ActionSqoopTag {

	private SqoopTag sqTag;
	private String name;
	private OkTo okTo;
	private ErrorTo errorTo;

	public SqoopTag getSqTag() {
		return sqTag;
	}

	@XmlElement(name="sqoop")
	public void setSqTag(SqoopTag sqImport) {
		this.sqTag = sqImport;
	}

	public String getName() {
		return name;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	public OkTo getOkTo() {
		return okTo;
	}

	public ErrorTo getErrorTo() {
		return errorTo;
	}

	@XmlElement(name="ok")
	public void setOkTo(OkTo okTo) {
		this.okTo = okTo;
	}

	@XmlElement(name="error")
	public void setErrorTo(ErrorTo errorTo) {
		this.errorTo = errorTo;
	}
}
