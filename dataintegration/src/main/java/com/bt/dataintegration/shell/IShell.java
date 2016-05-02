package com.bt.dataintegration.shell;

import com.bt.dataintegration.property.config.HadoopConfig;

public interface IShell {
	
	public void shellToHDFS(HadoopConfig conf);
	public void shellToHDFSFile(HadoopConfig conf);
}
