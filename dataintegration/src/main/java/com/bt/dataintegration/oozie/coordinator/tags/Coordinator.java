package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder={"name","frequency","start","end","timezone","xmlns","controls","action"})
@XmlRootElement(name="coordinator-app")
public class Coordinator {

	private String name;
	private String frequency;
	private String start;
	private String end;
	private String timezone;
	private String xmlns;
	private Controls controls;
	private Action action;

	public String getName() {
		return name;
	}

	public String getFrequency() {
		return frequency;
	}

	public String getStart() {
		return start;
	}

	public String getEnd() {
		return end;
	}

	public String getTimezone() {
		return timezone;
	}

	public String getXmlns() {
		return xmlns;
	}

	public Controls getControls() {
		return controls;
	}

	public Action getAction() {
		return action;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute(name="frequency")
	public void setFrequency(String frequency) {
		this.frequency = frequency;
	}

	@XmlAttribute(name="start")
	public void setStart(String start) {
		this.start = start;
	}

	@XmlAttribute(name="end")
	public void setEnd(String end) {
		this.end = end;
	}

	@XmlAttribute(name="timezone")
	public void setTimezone(String timezone) {
		this.timezone = timezone;
	}

	@XmlAttribute(name="xmlns")
	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}

	@XmlElement(name="controls")
	public void setControls(Controls controls) {
		this.controls = controls;
	}

	@XmlElement(name="action")
	public void setAction(Action action) {
		this.action = action;
	}

}
