package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * @author Abhinav Meghmala 609349708
 */
@XmlType(propOrder = {"name", "dataset", "instance"})
@XmlRootElement(name="data-in")
public class DataIn {

	private String name;
	private String dataset;
	private String instance;

	public String getName() {
		return name;
	}

	public String getDataset() {
		return dataset;
	}

	public String getInstance() {
		return instance;
	}

	@XmlAttribute(name="name")
	public void setName(String name) {
		this.name = name;
	}

	@XmlAttribute(name="dataset")
	public void setDataset(String dataset) {
		this.dataset = dataset;
	}

	@XmlElement(name="instance")
	public void setInstance(String instance) {
		this.instance = instance;
	}

}
