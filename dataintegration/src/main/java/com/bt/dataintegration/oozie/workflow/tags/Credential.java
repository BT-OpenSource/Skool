package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="credential")
public class Credential {

	private List<CredentialProperty> propList = new LinkedList<CredentialProperty>();
	private String name;
	private String type;

	public List<CredentialProperty> getPropList() {
		return propList;
	}

	@XmlElement(name="property")
	public void setPropList(List<CredentialProperty> propList) {
		this.propList = propList;
	}

	public String getName() {
		return name;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	@XmlAttribute(name="type")
	public void setType(String type) {
		this.type = type;
	}
}
