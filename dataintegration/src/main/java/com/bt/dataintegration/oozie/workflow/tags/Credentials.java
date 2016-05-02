package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708 (Abhinav Meghmala)
 */
@XmlRootElement(name = "credentials")
public class Credentials {

	private Credential hiveCredential;
	/*private String name;
	private String type;*/

	public Credential getHiveCredential() {
		return hiveCredential;
	}

	@XmlElement(name = "credential")
	public void setHiveCredential(Credential hiveCredential) {
		this.hiveCredential = hiveCredential;
	}

	/*public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute(name="type")
	public void setType(String type) {
		this.type = type;
	}*/

}
