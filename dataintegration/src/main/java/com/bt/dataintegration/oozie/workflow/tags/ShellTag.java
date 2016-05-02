package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = { "jobTracker", "nameNode", "exec", "argument", "sshFile",
		"captureOutput" })
@XmlRootElement(name = "shell")
public class ShellTag {

	private String xmlns;
	private String jobTracker;
	private String nameNode;
	private String exec;
	private LinkedList<String> argument;
	private String sshFile;
	private String captureOutput;

	public String getXmlns() {
		return xmlns;
	}

	public String getJobTracker() {
		return jobTracker;
	}

	public String getNameNode() {
		return nameNode;
	}

	public String getExec() {
		return exec;
	}

	public LinkedList<String> getArgument() {
		return argument;
	}

	public String getSshFile() {
		return sshFile;
	}

	public String getCaptureOutput() {
		return captureOutput;
	}

	@XmlAttribute(name="xmlns")
	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}

	@XmlElement(name="job-tracker")
	public void setJobTracker(String jobTracker) {
		this.jobTracker = jobTracker;
	}

	@XmlElement(name="name-node")
	public void setNameNode(String nameNode) {
		this.nameNode = nameNode;
	}

	@XmlElement(name="exec")
	public void setExec(String exec) {
		this.exec = exec;
	}

	@XmlElement(name="argument")
	public void setArgument(LinkedList<String> argument) {
		this.argument = argument;
	}

	@XmlElement(name="file")
	public void setSshFile(String sshFile) {
		this.sshFile = sshFile;
	}

	@XmlElement(name="capture-output")
	public void setCaptureOutput(String captureOutput) {
		this.captureOutput = captureOutput;
	}

}