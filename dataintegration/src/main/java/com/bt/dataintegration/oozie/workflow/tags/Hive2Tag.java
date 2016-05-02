package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"jobTracker", "nameNode", /*"jobXml",*/ "config", "jdbcUrl", "script", "params"})
@XmlRootElement(name="hive2")
public class Hive2Tag {

	private String xmlns;
	private String jobTracker;
	private String nameNode;
	//private String jobXml;
	private HiveConfiguration config;
	private String jdbcUrl;
	private String script;
	private List<String> params = new LinkedList<String>();

	public String getXmlns() {
		return xmlns;
	}

	@XmlAttribute(name="xmlns")
	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}

	public String getJobTracker() {
		return jobTracker;
	}

	@XmlElement(name="job-tracker")
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	public String getNameNode() {
		return nameNode;
	}

	@XmlElement(name="name-node")
	public void setNameNode(String nameNode) {
		this.nameNode = nameNode;
	}

	/*public String getJobXml() {
		return jobXml;
	}

	@XmlElement(name="job-xml")
	public void setJobXml(String jobXml) {
		this.jobXml = jobXml;
	}*/

	public HiveConfiguration getConfig() {
		return config;
	}

	@XmlElement(name="configuration")
	public void setConfig(HiveConfiguration config) {
		this.config = config;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	@XmlElement(name="jdbc-url")
	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public String getScript() {
		return script;
	}

	@XmlElement(name="script")
	public void setScript(String script) {
		this.script = script;
	}

	public List<String> getParams() {
		return params;
	}

	@XmlElement(name="param")
	public void setParams(List<String> params) {
		this.params = params;
	}
}
