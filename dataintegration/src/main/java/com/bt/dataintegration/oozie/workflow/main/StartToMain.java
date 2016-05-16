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
		
		if(SQOOP_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())) {
			sto.setStartTo(ACTION_REFRESH_LAST_MODIFIED_DATE_VALUE);
		}
		if(FILE_IMPORT.equalsIgnoreCase(hconf.getImport_export_flag())){
			sto.setStartTo(ACTION_CAPTURE_DATE_AND_CREATEDIR);
		}
		
		return sto;
	}
}
