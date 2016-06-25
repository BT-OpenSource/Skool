package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.oozie.workflow.tags.StartTo;
import com.bt.dataintegration.property.config.HadoopConfig;
import static com.bt.dataintegration.constants.Constants.*;
/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class StartToMain {

	private StartTo sto = new StartTo();
	
	public StartTo setStartTo(HadoopConfig hconf) {
		String ieFlag=hconf.getImport_export_flag();
		if(SQOOP_IMPORT.equalsIgnoreCase(ieFlag)) {
			sto.setStartTo(ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE);
		}
		if(FILE_IMPORT.equalsIgnoreCase(ieFlag)){
			sto.setStartTo(ACTION_CAPTURE_DATE_AND_CREATEDIR);
		}
		if(SQOOP_EXPORT.equalsIgnoreCase(ieFlag)) {
			/*if("".equalsIgnoreCase(hconf.getExport_user_dir())) {
				sto.setStartTo(ACTION_HIVE_EXTRACT_DATA);
			} else {
				sto.setStartTo(ACTION_SQOOP_EXPORT_TO_RDBMS_TABLE);
			}*/
			sto.setStartTo(ACTION_EXPORT_SHELL_STARTTO);
		}
		
		return sto;
	}
}
