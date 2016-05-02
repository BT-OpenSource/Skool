package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"emailTo", "subject", "body"})
@XmlRootElement(name="email")
public class EmailNotification {

	private String emailTo;
	private String subject;
	private String body;
	private String xmlns;

	public String getEmailTo() {
		return emailTo;
	}

	@XmlElement(name="to")
	public void setEmailTo(String emailTo) {
		this.emailTo = emailTo;
	}

	public String getSubject() {
		return subject;
	}

	@XmlElement(name="subject")
	public void setSubject(String subject) {
		this.subject = subject;
	}

	public String getBody() {
		return body;
	}

	@XmlElement(name="body")
	public void setBody(String body) {
		this.body = body;
	}

	public String getXmlns() {
		return xmlns;
	}

	@XmlAttribute(name="xmlns")
	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}
}
