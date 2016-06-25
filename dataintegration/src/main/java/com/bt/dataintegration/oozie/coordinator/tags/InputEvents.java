package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="input-events")
public class InputEvents {

	private DataIn din;

	public DataIn getDin() {
		return din;
	}

	@XmlElement(name="data-in")
	public void setDin(DataIn din) {
		this.din = din;
	}

}
