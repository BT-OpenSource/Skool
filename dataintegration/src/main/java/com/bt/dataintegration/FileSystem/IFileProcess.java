package com.bt.dataintegration.FileSystem;

import com.bt.dataintegration.property.config.DIConfig;

public interface IFileProcess {
	public String checkHiveTableExists(DIConfig conf);
	public void prepareJobProperties(DIConfig conf);
}
