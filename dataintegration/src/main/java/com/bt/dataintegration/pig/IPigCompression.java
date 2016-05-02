package com.bt.dataintegration.pig;

import com.bt.dataintegration.property.config.HadoopConfig;

public interface IPigCompression {

	public void compressData(HadoopConfig conf);
}
