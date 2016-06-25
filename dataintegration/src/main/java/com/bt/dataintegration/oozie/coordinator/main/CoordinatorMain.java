package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Action;
import com.bt.dataintegration.oozie.coordinator.tags.Controls;
import com.bt.dataintegration.oozie.coordinator.tags.Coordinator;
import com.bt.dataintegration.oozie.coordinator.tags.Datasets;
import com.bt.dataintegration.oozie.coordinator.tags.InputEvents;
import com.bt.dataintegration.oozie.coordinator.tags.SLAInfo;
import com.bt.dataintegration.property.config.HadoopConfig;

public class CoordinatorMain {

	public Coordinator setCoordinatorMain(Controls controls, Action action, HadoopConfig hconf, Datasets ds, InputEvents ie) {
		
		Coordinator coord = new Coordinator();
		String ieFlag = hconf.getImport_export_flag();
		coord.setAction(action);
		coord.setControls(controls);
		coord.setEnd("${end}");
		coord.setFrequency("${frequency}");
		if("1".equalsIgnoreCase(ieFlag)) {
			coord.setName("SKOOL_COORDINATOR_FOR_IMPORT_FROM_" + hconf.getTableName());
		} else if("2".equalsIgnoreCase(ieFlag)) {
			coord.setName("SKOOL_COORDINATOR_FOR_EXPORT_TO_" + hconf.getTableName());
		} else if("3".equalsIgnoreCase(ieFlag)) {
			coord.setDs(ds);
			coord.setIe(ie);
		}
		coord.setStart("${start}");
		coord.setTimezone("${timezone}");
		coord.setXmlns("uri:oozie:coordinator:0.2");
		if(hconf.isSlaRequired()){
			coord.setXmlns("uri:oozie:coordinator:0.4");
			coord.setXmlnssla("uri:oozie:sla:0.2");
		}
		return coord;
	}
}
