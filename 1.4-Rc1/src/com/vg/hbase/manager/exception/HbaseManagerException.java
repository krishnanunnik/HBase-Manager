package com.vg.hbase.manager.exception;

import org.apache.commons.lang.exception.ExceptionUtils;


/**
	
	Exceptions Class in PW WebServer
 * @author skrishnanunni
 * @date 30-Jul-2012 : 10:45:12 AM
 */
public class HbaseManagerException extends Exception {

	/**
	 * Exceptions in PW Servlets
	 */
	private static final long serialVersionUID = 1L;

	private String exceptionMessage;
	private String exceptionStackTrace;
	
	public static String ERROR_CODE_1 = "?Error=1"; //Invalid Credentials
	public static String ERROR_CODE_2 = "?Error=2"; //Inactive User
	public static String ERROR_CODE_3 = "?Error=3"; //Please Try after Sometime
	public static String ERROR_CODE_4 = "?Error=4"; //Unauthorized Access

	public String getExceptionMessage() {
		return exceptionMessage;
	}

	public String getExceptionStackTrace() {
		return exceptionStackTrace;
	}

	public HbaseManagerException() {
		super();
		this.exceptionMessage = "Exception";
		this.exceptionStackTrace = "";
	}

	public HbaseManagerException(String exMessage) {
		super(exMessage);
		this.exceptionMessage = exMessage;
	}

	public HbaseManagerException(Throwable ex) {
		super(ex);
		this.exceptionMessage = ex.getMessage();
		this.exceptionStackTrace = ExceptionUtils.getFullStackTrace(ex);

	}

	public HbaseManagerException(String exMessage, Throwable ex) {
		super(exMessage, ex);
		this.exceptionMessage = exMessage;
		this.exceptionStackTrace = ExceptionUtils.getFullStackTrace(ex);

	}

	
}
