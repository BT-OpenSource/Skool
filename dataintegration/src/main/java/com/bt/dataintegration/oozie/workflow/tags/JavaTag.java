package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
@XmlType(propOrder={"jobTracker", "nameNode", "conf", "mainClass", "arg"})
@XmlRootElement(name="java")
public class JavaTag {

	private String jobTracker;
	private String nameNode;
	private GlobalConfigurationProperty conf;
	private String mainClass;
	private LinkedList<String> arg;

	public String getJobTracker() {
		return jobTracker;
	}

	public String getNameNode() {
		return nameNode;
	}

	public GlobalConfigurationProperty getConf() {
		return conf;
	}

	public String getMainClass() {
		return mainClass;
	}

	public LinkedList<String> getArg() {
		return arg;
	}

	@XmlElement(name = "job-tracker")
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	@XmlElement(name="name-node")
	public void setNameNode(String nameNode) {
		this.nameNode = nameNode;
	}

	@XmlElement(name="configuration")
	public void setConf(GlobalConfigurationProperty conf) {
		this.conf = conf;
	}

	@XmlElement(name="main-class")
	public void setMainClass(String mainClass) {
		this.mainClass = mainClass;
	}

	@XmlElement(name="arg")
	public void setArg(LinkedList<String> arg) {
		this.arg = arg;
	}
}
