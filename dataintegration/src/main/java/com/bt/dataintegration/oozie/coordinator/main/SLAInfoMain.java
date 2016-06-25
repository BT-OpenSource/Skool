package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.SLAInfo;
import com.bt.dataintegration.property.config.HadoopConfig;

import static com.bt.dataintegration.constants.Constants.*;

public class SLAInfoMain {

	public SLAInfo setSLAInfoMain(HadoopConfig hconf) {
		SLAInfo si = new SLAInfo();
		String slaStart = "${"+hconf.getSlaStart()+ " * MINUTES}";
		String slaEnd = "${"+hconf.getSlaEnd()+ " * MINUTES}";		
		si.setSlaNominalTime(SLA_NOMINAL_TIME);
		si.setSlaStart(slaStart);
		si.setSlaEnd(slaEnd);
		si.setSlaMessage(SLA_MESSAGE);
		si.setSlaContact("${sla_contact}");
		
		return si;
	}
}
