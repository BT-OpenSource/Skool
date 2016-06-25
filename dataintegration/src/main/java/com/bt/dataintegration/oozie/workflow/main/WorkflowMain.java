package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.oozie.workflow.tags.ActionEmailNotification;
import com.bt.dataintegration.oozie.workflow.tags.ActionFS;
import com.bt.dataintegration.oozie.workflow.tags.ActionHive2;
import com.bt.dataintegration.oozie.workflow.tags.ActionJava;
import com.bt.dataintegration.oozie.workflow.tags.ActionPig;
import com.bt.dataintegration.oozie.workflow.tags.ActionShell;
import com.bt.dataintegration.oozie.workflow.tags.ActionSqoopTag;
import com.bt.dataintegration.oozie.workflow.tags.Credentials;
import com.bt.dataintegration.oozie.workflow.tags.EndTag;
import com.bt.dataintegration.oozie.workflow.tags.Global;
import com.bt.dataintegration.oozie.workflow.tags.KillTag;
import com.bt.dataintegration.oozie.workflow.tags.StartTo;
import com.bt.dataintegration.oozie.workflow.tags.Workflow;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class WorkflowMain implements Constants {
	
	private Workflow workflow = new Workflow();

	/*Take all actions as argument
	All main classes will return the action 
	type post call which will be passed to WorkflowMain method*/
	
	public Workflow setWorkflowMain(Global global, Credentials creds, ActionSqoopTag sqoopAction,
			StartTo startTo, ActionFS fsAction, ActionHive2 hiveAction, 
			ActionEmailNotification emailSuccess, ActionEmailNotification emailFailure, 
			KillTag kt, EndTag et, ActionJava javaAction, ActionPig pigAction, ActionHive2 hs2AddPart, 
			ActionShell aShell, HadoopConfig hconf, ActionShell shellInit, ActionShell auditLogs, 
			ActionShell housekeeping, ActionHive2 createAuditTable, ActionShell shellError,
			ActionJava javaAct) {
		
		String tableName = null;
		if(hconf.getEnvDetails().equals("1")) {
			
			workflow.setHiveCreds(creds);
		}
		workflow.setXmlns(WORKFLOW_XMLNS);
		workflow.setGlobal(global);
		workflow.setSt(startTo);
		if("1".equalsIgnoreCase(hconf.getImport_export_flag()) || "3".equalsIgnoreCase(hconf.getImport_export_flag())) {
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getTableName();
		} else if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getHiveTableName();
		}
		
		String flag = hconf.getImport_export_flag();
		//System.out.println(flag);
		if(SQOOP_IMPORT.equals(flag)) {
			workflow.setName("SKOOL_IMPORT_FROM_" + tableName);
		} else if(FILE_IMPORT.equals(flag)) {
			workflow.setName("SKOOL_FILESYSTEM_IMPORT_" + tableName);
		}
	
		
		//DIConfig conf = new DIConfig().getDIConfigProperties();
	
		if(SQOOP_IMPORT.equals(flag)) {
			workflow.setSqTag(sqoopAction);
		} else if(FILE_IMPORT.equals(flag)) {
			workflow.setJavaFSValidate(javaAct);
		}		
		
		workflow.setHs2(hiveAction);
		workflow.setHs2AddPart(hs2AddPart);
		workflow.setPigCompress(pigAction);
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			workflow.setShellRefresh(aShell);
		}
		//workflow.setJavaSetProp(javaAction);		
		/*workflow.setShellInit(shellInit);
		workflow.setAuditTable(auditLogs);
		workflow.setCreateAuditTable(createAuditTable);
		workflow.setShellErr(shellError);*/
		
		if((hconf.isHousekeepRequired() && (SQOOP_IMPORT.equals(flag))) || (hconf.isHousekeepRequired() && (FILE_IMPORT.equals(flag)))) {
			workflow.setHousekeeping(housekeeping);
		}
		}
		
		workflow.setShellInit(shellInit);
		workflow.setAuditTable(auditLogs);
		workflow.setCreateAuditTable(createAuditTable);
		workflow.setShellErr(shellError);
		workflow.setSuccess(emailSuccess);
		workflow.setFailure(emailFailure);
		workflow.setKt(kt);
		workflow.setEt(et);
		if("2".equalsIgnoreCase(hconf.getImport_export_flag())) {
			tableName = hconf.getTableName();
			workflow.setName("SKOOL_EXPORT_TO_" + tableName);			
			workflow.setSqTag(sqoopAction);
			
			if("".equalsIgnoreCase(hconf.getExport_user_dir())) {
				workflow.setHs2(hiveAction);
				workflow.setFsc(fsAction);
			}
			
		}
		return workflow;
	}
}
