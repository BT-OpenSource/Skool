package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Action;
import com.bt.dataintegration.oozie.coordinator.tags.Controls;
import com.bt.dataintegration.oozie.coordinator.tags.Coordinator;
import com.bt.dataintegration.property.config.HadoopConfig;

public class CoordinatorMain {

	public Coordinator setCoordinatorMain(Controls controls, Action action, HadoopConfig hconf) {
		
		Coordinator coord = new Coordinator();
		coord.setAction(action);
		coord.setControls(controls);
		coord.setEnd("${end}");
		coord.setFrequency("${frequency}");
		coord.setName("COORDINATOR_FOR_DATA_IMPORT_FROM_" + hconf.getTableName());
		coord.setStart("${start}");
		coord.setTimezone("${timezone}");
		coord.setXmlns("uri:oozie:coordinator:0.2");
		return coord;
	}
}
