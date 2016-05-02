package com.bt.dataintegration.oozie.workflow.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"name", /*"retryMax", "retryInt",*/ "ptag", "okt", "ert"})
@XmlRootElement(name="action")
public class ActionPig {

	private String name;
	/*private String retryMax;
	private String retryInt;*/
	private PigTag ptag;
	private OkTo okt;
	private ErrorTo ert;

	public String getName() {
		return name;
	}

	/*public String getRetryMax() {
		return retryMax;
	}

	public String getRetryInt() {
		return retryInt;
	}*/

	public PigTag getPtag() {
		return ptag;
	}

	public OkTo getOkt() {
		return okt;
	}

	public ErrorTo getErt() {
		return ert;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	/*@XmlAttribute(name="retry-max")
	public void setRetryMax(String retryMax) {
		this.retryMax = retryMax;
	}

	@XmlAttribute(name="retry-interval")
	public void setRetryInt(String retryInt) {
		this.retryInt = retryInt;
	}*/

	@XmlElement(name="pig")
	public void setPtag(PigTag ptag) {
		this.ptag = ptag;
	}

	@XmlElement(name="ok")
	public void setOkt(OkTo okt) {
		this.okt = okt;
	}

	@XmlElement(name="error")
	public void setErt(ErrorTo ert) {
		this.ert = ert;
	}

}
