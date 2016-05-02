/*
 * Author: 609298143 (Manish Bajaj)
 */

package com.bt.dataintegration.property.config;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

//Setting database metadata details
public class TableProperties {
	private Double tableSize;
	private LinkedHashMap<String, List<String>> colDetails;
	private Map<String, String> colCons;
	private Map<String, List<String>> colPartition;

	public TableProperties() {
		super();
	}

	//Maintain the sequence of retrieved columns from database
	public LinkedHashMap<String, List<String>> getColDetails() {
		return colDetails;
	}


	public void setColDetails(LinkedHashMap<String, List<String>> colDetails) {
		this.colDetails = colDetails;
	}


	public Double getTableSize() {
		return tableSize;
	}

	public void setTableSize(Double tableSize) {
		this.tableSize = tableSize;
	}

	public Map<String, String> getColCons() {
		return colCons;
	}

	public void setColCons(Map<String, String> colCons) {
		this.colCons = colCons;
	}

	public Map<String, List<String>> getColPartition() {
		return colPartition;
	}

	public void setColPartition(Map<String, List<String>> colPartition) {
		this.colPartition = colPartition;
	}
}
