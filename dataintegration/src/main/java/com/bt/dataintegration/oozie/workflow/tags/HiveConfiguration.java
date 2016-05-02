package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="configuration")
public class HiveConfiguration {

	private LinkedList<HiveConfigProperty> property;

	public LinkedList<HiveConfigProperty> getProperty() {
		return property;
	}

	@XmlElement(name="property")
	public void setProperty(LinkedList<HiveConfigProperty> property) {
		this.property = property;
	}
}
