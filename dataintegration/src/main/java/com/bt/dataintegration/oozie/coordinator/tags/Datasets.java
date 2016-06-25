package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="datasets")
public class Datasets {

	private Dataset ds;

	public Dataset getDs() {
		return ds;
	}

	@XmlElement(name="dataset")
	public void setDs(Dataset ds) {
		this.ds = ds;
	}

}
