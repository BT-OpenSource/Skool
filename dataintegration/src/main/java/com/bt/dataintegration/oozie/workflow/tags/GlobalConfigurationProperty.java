package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="property")
public class GlobalConfigurationProperty {

	private String name;
	private String value;
	
	public String getName() {
		return name;
	}
	
	@XmlElement(name="name")
	public void setName(String name) {
		this.name = name;
	}
		
	public String getValue() {
		return value;
	}
	
	@XmlElement(name="value")
	public void setValue(String value) {
		this.value = value;
	}	
}
