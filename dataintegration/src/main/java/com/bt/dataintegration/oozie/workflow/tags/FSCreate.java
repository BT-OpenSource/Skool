package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="fs")
public class FSCreate {

	private CreatePath cpath;

	public CreatePath getCpath() {
		return cpath;
	}

	@XmlElement(name="mkdir")
	public void setCpath(CreatePath cpath) {
		this.cpath = cpath;
	}
}
