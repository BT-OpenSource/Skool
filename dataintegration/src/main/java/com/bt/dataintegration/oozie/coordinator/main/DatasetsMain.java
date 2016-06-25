package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Dataset;
import com.bt.dataintegration.oozie.coordinator.tags.Datasets;

public class DatasetsMain {

	public Datasets setDatasetsMain() {
		
		Datasets dSets = new Datasets();
		Dataset ds = new Dataset();
		
		ds.setDoneFlag("${control_file_name}");
		ds.setFrequency("${polling_frequency}");
		ds.setInitialInstance("${start}");
		ds.setTimezone("${timezone}");
		ds.setName("input");
		ds.setUriTemplate("${file_base_directory}");
		
		dSets.setDs(ds);
		
		return dSets;
	}
}
