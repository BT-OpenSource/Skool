package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"jobTracker", "nameNode", "conf", /*"jobXml", */"script", "param", "fileName"})
@XmlRootElement(name="pig")
public class PigTag {

	private String jobTracker;
	private String nameNode;
	//private String jobXml;
	private String script;
	private LinkedList<String> param;
	private String fileName;
	private GlobalConfiguration conf;	

	public String getJobTracker() {
		return jobTracker;
	}

	public String getNameNode() {
		return nameNode;
	}

	/*public String getJobXml() {
		return jobXml;
	}*/

	public String getScript() {
		return script;
	}

	public LinkedList<String> getParam() {
		return param;
	}

	public String getFileName() {
		return fileName;
	}

	@XmlElement(name="job-tracker")
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	@XmlElement(name="name-node")
	public void setNameNode(String nameNode) {
		this.nameNode = nameNode;
	}

	/*@XmlElement(name="job-xml")
	public void setJobXml(String jobXml) {
		this.jobXml = jobXml;
	}*/

	@XmlElement(name="script")
	public void setScript(String script) {
		this.script = script;
	}

	@XmlElement(name="param")
	public void setParam(LinkedList<String> param) {
		this.param = param;
	}

	@XmlElement(name="file")
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public GlobalConfiguration getConf() {
		return conf;
	}

	@XmlElement(name="configuration")
	public void setConf(GlobalConfiguration conf) {
		this.conf = conf;
	}
}
