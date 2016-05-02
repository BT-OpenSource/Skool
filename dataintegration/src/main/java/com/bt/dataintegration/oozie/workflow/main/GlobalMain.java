package com.bt.dataintegration.oozie.workflow.main;

import com.bt.dataintegration.constants.Constants;
import com.bt.dataintegration.oozie.workflow.tags.Global;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfiguration;
import com.bt.dataintegration.oozie.workflow.tags.GlobalConfigurationProperty;

/**
 * @author 609349708
 *	(Abhinav Meghmala)
 */
public class GlobalMain implements Constants {

	private GlobalConfigurationProperty gprop = new GlobalConfigurationProperty();
	private GlobalConfiguration conf = new GlobalConfiguration();
	private Global global = new Global();
	
	public Global setGlobalMain() {
		
		gprop.setName(GCP_QUEUENAME);
		gprop.setValue("${queueName}");
		conf.setProperty(gprop);
		global.setConfiguration(conf);
		
		return global;
	}
}
