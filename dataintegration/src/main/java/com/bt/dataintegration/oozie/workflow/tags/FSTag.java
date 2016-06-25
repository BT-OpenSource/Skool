package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlRootElement(name="fs")
public class FSTag {

	private FSdelete delete;

	public FSdelete getDelete() {
		return delete;
	}

	@XmlElement(name="delete")
	public void setDelete(FSdelete delete) {
		this.delete = delete;
	}
	
}
