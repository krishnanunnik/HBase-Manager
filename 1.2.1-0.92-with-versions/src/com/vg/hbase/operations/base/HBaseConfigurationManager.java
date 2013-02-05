package com.vg.hbase.operations.base;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * @author kvijay
 * 
 */
public class HBaseConfigurationManager {

	public static Configuration hbaseConf = null;
	public static HBaseAdmin hbaseAdmin = null;

	public HBaseConfigurationManager(String newConf) {

		hbaseConf = getConfiguration();

	}

	public HBaseConfigurationManager() {
		if (hbaseConf == null) {
			hbaseConf = getConfiguration();
		}
	}

	/**
	 * @return new HBaseConfiguration
	 */
	private Configuration getConfiguration() {
		Configuration config = HBaseConfiguration.create();
		config.clear();

		config.set("hbase.zookeeper.quorum",
				HbaseManagerStatic.getHBASE_ZOOKEEPER_QUORUM());
		config.set("hbase.zookeeper.property.clientPort",
				HbaseManagerStatic.getHBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT());
		config.set("hbase.master", HbaseManagerStatic.getHBASE_MASTER());

		try {
			hbaseAdmin = new HBaseAdmin(config);
		} catch (MasterNotRunningException e) {

			HbaseManagerStatic.SERVER_ERROR = true;

		} catch (ZooKeeperConnectionException e) {
			HbaseManagerStatic.SERVER_ERROR = true;
		} catch (Exception e) {
			HbaseManagerStatic.SERVER_ERROR = true;
		}
		return config;

	}

	public static Configuration getHbaseConf() {
		return hbaseConf;
	}

	public static HBaseAdmin getHbaseAdmin() {
		return hbaseAdmin;

	}

	public static void setHbaseConf(HBaseConfiguration hbaseConf) {
		HBaseConfigurationManager.hbaseConf = hbaseConf;
	}

}
