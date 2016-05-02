package com.bt.dataintegration.oozie.workflow.xmlcodegen;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.log4j.Logger;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.hive.HiveProcessImpl;
import com.bt.dataintegration.oozie.workflow.main.CredentialsMain;
import com.bt.dataintegration.oozie.workflow.main.EmailFailureMain;
import com.bt.dataintegration.oozie.workflow.main.EmailSuccessMain;
import com.bt.dataintegration.oozie.workflow.main.EndMain;
import com.bt.dataintegration.oozie.workflow.main.FSMain;
import com.bt.dataintegration.oozie.workflow.main.GlobalMain;
import com.bt.dataintegration.oozie.workflow.main.Hive2Main;
import com.bt.dataintegration.oozie.workflow.main.JavaMain;
import com.bt.dataintegration.oozie.workflow.main.KillMain;
import com.bt.dataintegration.oozie.workflow.main.PigMain;
import com.bt.dataintegration.oozie.workflow.main.ShellMain;
import com.bt.dataintegration.oozie.workflow.main.SqoopMain;
import com.bt.dataintegration.oozie.workflow.main.StartToMain;
import com.bt.dataintegration.oozie.workflow.main.WorkflowMain;
import com.bt.dataintegration.oozie.workflow.tags.ActionEmailNotification;
import com.bt.dataintegration.oozie.workflow.tags.ActionFSCreate;
import com.bt.dataintegration.oozie.workflow.tags.ActionHive2;
import com.bt.dataintegration.oozie.workflow.tags.ActionJava;
import com.bt.dataintegration.oozie.workflow.tags.ActionPig;
import com.bt.dataintegration.oozie.workflow.tags.ActionShell;
import com.bt.dataintegration.oozie.workflow.tags.ActionSqoopImport;
import com.bt.dataintegration.oozie.workflow.tags.Credentials;
import com.bt.dataintegration.oozie.workflow.tags.EndTag;
import com.bt.dataintegration.oozie.workflow.tags.Global;
import com.bt.dataintegration.oozie.workflow.tags.KillTag;
import com.bt.dataintegration.oozie.workflow.tags.StartTo;
import com.bt.dataintegration.oozie.workflow.tags.Workflow;
import com.bt.dataintegration.property.config.HadoopConfig;
import com.bt.dataintegration.utilities.DirectoryHandler;
import com.bt.dataintegration.utilities.Utility;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class WorkflowXMLCodegen implements Constants {

	private Global global = null;
	private Credentials creds = null;
	private ActionSqoopImport sqoopAction = null;
	private StartTo startTo = null;
	private ActionFSCreate fsAction = null;
	private ActionHive2 hiveAction = null;
	private ActionEmailNotification emailSuccess = null;
	private ActionEmailNotification emailFailure = null;
	private KillTag kt = null;
	private EndTag et = null;
	private ActionJava javaAction = null;
	private ActionPig pigAction = null;
	private WorkflowMain wfMain = new WorkflowMain();
	private Workflow workflow = null;
	private ActionHive2 addPartition = null;
	//private HadoopConfig hconf = new HadoopConfig().getHadoopConfigProperties();
	private ActionShell aShell = null;
	private ActionShell shellInit = null;
	private ActionShell auditLogs = null;
	private ActionShell housekeeping = null;
	private ActionHive2 createAuditTable = null;
	private ActionShell shellErr = null;
	final static Logger logger = Logger.getLogger(WorkflowXMLCodegen.class);
	
	//Method de-scoped as not used for Hive JDBC anymore
	/*public void copyHivesiteXmlToHDFS(HadoopConfig hconf) {
		logger.info("Copying hive-site.xml to workspace...");
		String cmd = "hadoop fs -put " + HIVE_SITE_PATH + " "
				+ hconf.getWorkspacePath();
		
		logger.debug(cmd);
		int shellout = 1;
		shellout = Utility.executeSSH(cmd);
		if(shellout !=0){
			throw new Error();
		}
	}*/

	public void generateXML(HadoopConfig hconf) {

		global = new GlobalMain().setGlobalMain();
		creds = new CredentialsMain().setCredentialsMain(hconf);
		
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			sqoopAction = new SqoopMain().setSqoopMain(hconf);
			housekeeping = new ShellMain().setShellMainHousekeeping(hconf);
		} else if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
			javaAction = new JavaMain().setJavaMain(hconf);
		}
		startTo = new StartToMain().setStartTo(hconf);
		fsAction = new FSMain().setFSMain(hconf);
		hiveAction = new Hive2Main().setHive2MainHiveCreate(hconf);
		addPartition = new Hive2Main().setHive2MainPartition(hconf);
		emailSuccess = new EmailSuccessMain().setEmailSuccessMain(hconf);
		emailFailure = new EmailFailureMain().setEmailSuccessMain();
		kt = new KillMain().setKillMain();
		et = new EndMain().setEndMain();		
		pigAction = new PigMain().setPigMain(hconf);
		aShell = new ShellMain().setShellMain(hconf);
		shellInit = new ShellMain().setShellMainInit(hconf);
		auditLogs = new ShellMain().setShellMainAudit(hconf);		
		createAuditTable = new Hive2Main().setHive2MainCreateAuditTable(hconf);
		shellErr = new ShellMain().setShellMainError(hconf);
		
		logger.info("Setting Workflow Tags...");
		
		workflow = wfMain.setWorkflowMain(global, creds, sqoopAction, startTo, fsAction,
				hiveAction, emailSuccess, emailFailure, kt, et, javaAction,
				pigAction, addPartition, aShell, hconf, shellInit, auditLogs, housekeeping, 
				createAuditTable, shellErr, javaAction);

		try {

			logger.info("Generating Oozie workflow.xml...");
			//File file = new File(hconf.getTableName()+"/workflow.xml");
			String tableName = null;
			if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
				tableName = hconf.getTableName();
			} else if("3".equalsIgnoreCase(hconf.getImport_export_flag())) {
				tableName = hconf.getHiveTableName();
			}
			File file = new File(tableName + "/workflow.xml");
			JAXBContext jaxbContext = JAXBContext.newInstance(Workflow.class);
			javax.xml.bind.Marshaller jaxbMarshaller = jaxbContext
					.createMarshaller();

			// output pretty printed
			jaxbMarshaller.setProperty(
					javax.xml.bind.Marshaller.JAXB_FORMATTED_OUTPUT, true);

			jaxbMarshaller.marshal(workflow, file);
			//jaxbMarshaller.marshal(workflow, System.out);
			DirectoryHandler.sendFileToHDFS(hconf, "workflow.xml");
			DirectoryHandler.givePermissionToHDFSFile(hconf, "workflow.xml");

		} catch (JAXBException e) {
			logger.error("Error generating XML for oozie Workflow", e);
		} /*finally {
			
			String cmd = "hadoop fs -put "+hconf.getTableName()+"/workflow.xml "
					+ hconf.getWorkspacePath();
			logger.info("Putting workflow.xml in workspace...");
			logger.debug(cmd);
			int shellout = 1;
			shellout = Utility.executeSSH(cmd);
			if(shellout !=0){
				throw new Error();
			}
		}*/
	}
}
