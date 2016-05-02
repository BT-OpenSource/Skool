package com.bt.dataintegration.oozie.coordinator.tags;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder={"timeout","concurrency","throttle"})
@XmlRootElement(name="controls")
public class Controls {

	private String concurrency;
	private String throttle;
	private String timeout;

	public String getConcurrency() {
		return concurrency;
	}

	@XmlElement(name="concurrency")
	public void setConcurrency(String concurrency) {
		this.concurrency = concurrency;
	}

	public String getThrottle() {
		return throttle;
	}

	@XmlElement(name="throttle")
	public void setThrottle(String throttle) {
		this.throttle = throttle;
	}

	public String getTimeout() {
		return timeout;
	}

	@XmlElement(name="timeout")
	public void setTimeout(String timeout) {
		this.timeout = timeout;
	}

}
