package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.oozie.workflow.tags.StartTo;
import com.bt.dataintegration.property.config.HadoopConfig;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class StartToMain {

	private StartTo sto = new StartTo();
	
	public StartTo setStartTo(HadoopConfig hconf) {
		
		if("1".equalsIgnoreCase(hconf.getImport_export_flag())) {
			sto.setStartTo("REFRESH_LAST_MODIFIED_DATE_VALUE");
		}
		if("3".equalsIgnoreCase(hconf.getImport_export_flag())){
			sto.setStartTo("CAPTURE_DATE_AND_CREATEDIR");
		}
		
		return sto;
	}
}
