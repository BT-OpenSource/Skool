package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="global")
public class Global {

	private GlobalConfiguration configuration;

	public GlobalConfiguration getConfiguration() {
		return configuration;
	}

	@XmlElement(name="configuration")
	public void setConfiguration(GlobalConfiguration configuration) {
		this.configuration = configuration;
	}
}
