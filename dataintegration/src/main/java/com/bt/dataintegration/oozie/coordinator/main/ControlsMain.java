package com.bt.dataintegration.oozie.coordinator.main;

import com.bt.dataintegration.oozie.coordinator.tags.Controls;

public class ControlsMain {

	public Controls setControlsMain() {
		
		Controls controls = new Controls();
		controls.setConcurrency("${concurrency}");
		controls.setThrottle("${throttle}");
		controls.setTimeout("${timeout}");
		return controls;
	}
}
