package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = { "name", "frequency", "initialInstance", "timezone", "uriTemplate", "doneFlag" })
@XmlRootElement(name="dataset")
public class Dataset {

	private String name;
	private String frequency;
	private String initialInstance;
	private String timezone;
	private String uriTemplate;
	private String doneFlag;

	public String getName() {
		return name;
	}

	public String getFrequency() {
		return frequency;
	}

	public String getInitialInstance() {
		return initialInstance;
	}

	public String getTimezone() {
		return timezone;
	}

	public String getUriTemplate() {
		return uriTemplate;
	}

	public String getDoneFlag() {
		return doneFlag;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute(name="frequency")
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	@XmlAttribute(name="initial-instance")
	public void setInitialInstance(String initialInstance) {
		this.initialInstance = initialInstance;
	}

	@XmlAttribute(name="timezone")
	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}

	@XmlElement(name="uri-template")
	public void setUriTemplate(String uriTemplate) {
		this.uriTemplate = uriTemplate;
	}

	@XmlElement(name="done-flag")
	public void setDoneFlag(String doneFlag) {
		this.doneFlag = doneFlag;
	}

}
