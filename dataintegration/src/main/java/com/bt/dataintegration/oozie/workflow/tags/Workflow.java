package com.bt.dataintegration.oozie.workflow.tags;

import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author 609349708 (Abhinav Meghmala)
 */
@XmlType(propOrder = { "global", "hiveCreds", "st", "shellInit", "javaFSValidate",
		"sqTag", "pigCompress", "shellRefresh", "hs2", "hs2AddPart", "auditTable", 
		"createAuditTable", "housekeeping", "shellErr", "fsc", "success", "failure", "kt", "et" })
@XmlRootElement(name = "workflow-app")
public class Workflow {

	private String name;
	private String xmlns;
	private Global global;
	private Credentials hiveCreds;
	// private List<Object> actions = new LinkedList<Object>();
	private ActionSqoopTag sqTag;
	private StartTo st;
	private ActionFS fsc;
	private ActionHive2 hs2;
	private ActionEmailNotification success;
	private ActionEmailNotification failure;
	private KillTag kt;
	private EndTag et;
	private ActionJava javaFSValidate;
	private ActionPig pigCompress;
	private ActionHive2 hs2AddPart;
	private ActionShell shellRefresh;
	private ActionShell shellInit;
	private ActionShell auditTable;
	private ActionShell housekeeping;
	private ActionHive2 createAuditTable;
	private ActionShell shellErr;

	public Global getGlobal() {
		return global;
	}

	@XmlElement(name = "global")
	public void setGlobal(Global global) {
		this.global = global;
	}

	public String getName() {
		return name;
	}

	@XmlAttribute(name = "name")
	public void setName(String name) {
		this.name = name;
	}

	public String getXmlns() {
		return xmlns;
	}

	@XmlAttribute(name = "xmlns")
	public void setXmlns(String xmlns) {
		this.xmlns = xmlns;
	}

	public Credentials getHiveCreds() {
		return hiveCreds;
	}

	@XmlElement(name = "credentials")
	public void setHiveCreds(Credentials hiveCreds) {
		this.hiveCreds = hiveCreds;
	}

	public ActionSqoopTag getSqTag() {
		return sqTag;
	}

	@XmlElement(name = "action")
	public void setSqTag(ActionSqoopTag sqTag) {
		this.sqTag = sqTag;
	}

	public StartTo getSt() {
		return st;
	}

	@XmlElement(name = "start")
	public void setSt(StartTo st) {
		this.st = st;
	}

	public ActionFS getFsc() {
		return fsc;
	}

	@XmlElement(name = "action")
	public void setFsc(ActionFS fsc) {
		this.fsc = fsc;
	}

	public ActionHive2 getHs2() {
		return hs2;
	}

	@XmlElement(name = "action")
	public void setHs2(ActionHive2 hs2) {
		this.hs2 = hs2;
	}

	public ActionEmailNotification getSuccess() {
		return success;
	}

	@XmlElement(name = "action")
	public void setSuccess(ActionEmailNotification success) {
		this.success = success;
	}

	public ActionEmailNotification getFailure() {
		return failure;
	}

	@XmlElement(name = "action")
	public void setFailure(ActionEmailNotification failure) {
		this.failure = failure;
	}

	public KillTag getKt() {
		return kt;
	}

	public EndTag getEt() {
		return et;
	}

	@XmlElement(name = "kill")
	public void setKt(KillTag kt) {
		this.kt = kt;
	}

	@XmlElement(name = "end")
	public void setEt(EndTag et) {
		this.et = et;
	}

	public ActionJava getJavaFSValidate() {
		return javaFSValidate;
	}

	public ActionPig getPigCompress() {
		return pigCompress;
	}

	@XmlElement(name = "action")
	public void setJavaFSValidate(ActionJava javaFSValidate) {
		this.javaFSValidate = javaFSValidate;
	}

	@XmlElement(name = "action")
	public void setPigCompress(ActionPig pigCompress) {
		this.pigCompress = pigCompress;
	}

	public ActionHive2 getHs2AddPart() {
		return hs2AddPart;
	}

	@XmlElement(name = "action")
	public void setHs2AddPart(ActionHive2 hs2AddPart) {
		this.hs2AddPart = hs2AddPart;
	}

	public ActionShell getShellRefresh() {
		return shellRefresh;
	}

	@XmlElement(name="action")
	public void setShellRefresh(ActionShell shellRefresh) {
		this.shellRefresh = shellRefresh;
	}

	public ActionShell getShellInit() {
		return shellInit;
	}

	@XmlElement(name="action")
	public void setShellInit(ActionShell shellInit) {
		this.shellInit = shellInit;
	}

	public ActionShell getAuditTable() {
		return auditTable;
	}

	public ActionShell getHousekeeping() {
		return housekeeping;
	}

	public ActionHive2 getCreateAuditTable() {
		return createAuditTable;
	}

	@XmlElement(name="action")
	public void setAuditTable(ActionShell auditTable) {
		this.auditTable = auditTable;
	}

	@XmlElement(name="action")
	public void setHousekeeping(ActionShell housekeeping) {
		this.housekeeping = housekeeping;
	}

	@XmlElement(name="action")
	public void setCreateAuditTable(ActionHive2 createAuditTable) {
		this.createAuditTable = createAuditTable;
	}

	public ActionShell getShellErr() {
		return shellErr;
	}

	@XmlElement(name="action")
	public void setShellErr(ActionShell shellErr) {
		this.shellErr = shellErr;
	}
	
	
}
