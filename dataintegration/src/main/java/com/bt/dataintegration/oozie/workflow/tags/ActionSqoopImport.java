package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"sqImport", "okTo", "errorTo"})
@XmlRootElement(name="action")
public class ActionSqoopImport {

	private SqoopImport sqImport;
	private String name;
	private OkTo okTo;
	private ErrorTo errorTo;

	public SqoopImport getSqImport() {
		return sqImport;
	}

	@XmlElement(name="sqoop")
	public void setSqImport(SqoopImport sqImport) {
		this.sqImport = sqImport;
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
