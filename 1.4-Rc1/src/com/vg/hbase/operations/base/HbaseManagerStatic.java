
package com.vg.hbase.operations.base;

public final class HbaseManagerStatic {

	private static String HBASE_ZOOKEEPER_QUORUM;
	private static String HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
	private static String HBASE_MASTER;
	private static String currentConnectionAlias;

	public static boolean SERVER_ERROR = false;
	public static boolean SERVER_NOT_CONNECTED = true;

	public static String getHBASE_ZOOKEEPER_QUORUM() {

		return HbaseManagerStatic.HBASE_ZOOKEEPER_QUORUM;
	}

	public static void setHBASE_ZOOKEEPER_QUORUM(String hBASE_ZOOKEEPER_QUORUM) {

		HbaseManagerStatic.HBASE_ZOOKEEPER_QUORUM = hBASE_ZOOKEEPER_QUORUM;
	}

	public static String getHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT() {

		return HbaseManagerStatic.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
	}

	public static void setHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT(String hBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT) {

		HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT = hBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT;
	}

	public static String getHBASE_MASTER() {

		return HbaseManagerStatic.HBASE_MASTER;
	}

	public static void setHBASE_MASTER(String hBASE_MASTER) {

		HbaseManagerStatic.HBASE_MASTER = hBASE_MASTER;
	}

	public static void clearAllConfigs() {

		setHBASE_MASTER("");
		setHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT("");
		setHBASE_ZOOKEEPER_QUORUM("");
	}

	public static String getCurrentConnectionAlias() {

		return currentConnectionAlias;
	}

	public static void setCurrentConnectionAlias(String currentConnectionAlias) {

		HbaseManagerStatic.currentConnectionAlias = currentConnectionAlias;
	}

}
