package com.bt.dataintegration.oozie.workflow.tags;

import java.util.ArrayList;
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
@XmlType(propOrder={"jobTracker", "nameNode", "configuration", "args", "files"})
@XmlRootElement(name="sqoop")
public class SqoopTag {

	private String jobTracker;
	private String nameNode;
	private List<String> args = new LinkedList<String>();
	private List<String> files = new ArrayList<String>();
	private String xmlns;
	private GlobalConfiguration configuration =  new GlobalConfiguration();
	
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
	public List<String> getArgs() {
		return args;
	}
	
	@XmlElement(name="arg")
	public void setArgs(List<String> args) {
		this.args = args;
	}
	public List<String> getFiles() {
		return files;
	}
	
	@XmlElement(name="file")
	public void setFiles(List<String> files) {
		this.files = files;
	}

	public String getXmlns() {
		return xmlns;
	}

	@XmlAttribute(name="xmlns")
	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}

	public GlobalConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(GlobalConfiguration configuration) {
		this.configuration = configuration;
	}

}
