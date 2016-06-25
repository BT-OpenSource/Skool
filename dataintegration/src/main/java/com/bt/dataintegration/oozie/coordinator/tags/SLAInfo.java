package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = {"slaNominalTime", "slaStart", "slaEnd", "slaContact","slaMessage"})
@XmlRootElement(name="sla:info")
public class SLAInfo {

	private String slaNominalTime;
	private String slaStart;
	private String slaEnd;
	private String slaMessage;
	private String slaContact;
	
	public String getSlaNominalTime() {
		return slaNominalTime;
	}
	public String getSlaStart() {
		return slaStart;
	}
	public String getSlaEnd() {
		return slaEnd;
	}
	public String getSlaMessage() {
		return slaMessage;
	}
	public String getSlaContact() {
		return slaContact;
	}
	
	@XmlElement(name="sla:nominal-time")
	public void setSlaNominalTime(String slaNominalTime) {
		this.slaNominalTime = slaNominalTime;
	}
	
	@XmlElement(name="sla:should-start")
	public void setSlaStart(String slaStart) {
		this.slaStart = slaStart;
	}
	
	@XmlElement(name="sla:should-end")
	public void setSlaEnd(String slaEnd) {
		this.slaEnd = slaEnd;
	}
	
	@XmlElement(name="sla:alert-contact")
	public void setSlaContact(String slaContact) {
		this.slaContact = slaContact;
	}
	
	@XmlElement(name="sla:notification-msg")
	public void setSlaMessage(String slaMessage) {
		this.slaMessage = slaMessage;
	}
	
	
	
}
