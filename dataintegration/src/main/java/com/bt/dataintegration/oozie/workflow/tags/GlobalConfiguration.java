package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="configuration")
public class GlobalConfiguration {

	private GlobalConfigurationProperty property;

	public GlobalConfigurationProperty getProperty() {
		return property;
	}

	@XmlElement(name="property")
	public void setProperty(GlobalConfigurationProperty property) {
		this.property = property;
	}
}
