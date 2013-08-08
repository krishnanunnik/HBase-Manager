package com.vg.hbase.logger;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
Logger for PW Login/Registration	
 * @author skrishnanunni
 * @date 03-Oct-2012 : 11:44:14 AM
 */
public class PWServletLogger {

	private Logger pwLogger;
	public PWServletLogger(Class<?> className) 
	{
		this.pwLogger = Logger.getLogger(className);
		this.pwLogger.setLevel(Level.ALL);
		
	}
	
	public void logInfo(String message)
	{
		this.pwLogger.info(message);
	}
	public void logDebug(String message)
	{
		this.pwLogger.debug(message);
	}
	public void logWarn(String message)
	{
		this.pwLogger.warn(message);
	}
	public void logError(String message)
	{
		this.pwLogger.error(message);
	}

}
