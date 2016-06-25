package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.DataIn;
import com.bt.dataintegration.oozie.coordinator.tags.InputEvents;

public class InputEventsMain {

	public InputEvents setInputEventaMain() {
		
		InputEvents ie = new InputEvents();
		DataIn din= new DataIn();
		
		din.setName("sample_input");
		din.setDataset("input");
		din.setInstance("${start}");
		
		ie.setDin(din);
		
		return ie;
	}
}
