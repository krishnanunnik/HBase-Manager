/**
 * 
 */

package com.vg.hbase.comm.manager;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.vg.hbase.manager.ui.HBaseManagerViewTable;
import com.vg.hbase.operations.base.HBaseConfigurationManager;
import com.vg.hbase.operations.base.HbaseManagerStatic;

// import org.apache.hadoop.hbase.KeyValue;

/**
 * @author kvijay
 * 
 */
public class HBaseTableManager {
	static HashMap<String, HTable> tableMap = null;
	private static HBaseConfigurationManager hBaseConfigManager;
	private static String[] columnQualifiers;
	private static String[][] ResultMap;
	private static HBaseTableManager currentTableManager;

	public static String[] getColumnQualifiers() {

		String[] clone = columnQualifiers.clone();
		columnQualifiers = null;
		return clone;
	}

	public void setColumnQualifiers(String[] columnQualifiers) {

		HBaseTableManager.columnQualifiers = columnQualifiers;
	}

	public String[][] getResultMap() {

		String[][] clone = ResultMap.clone();
		ResultMap = null;
		return clone;

	}

	public HBaseTableManager() {

		if (hBaseConfigManager == null) {
			hBaseConfigManager = new HBaseConfigurationManager();
		}

		if (tableMap == null) {
			tableMap = new HashMap<String, HTable>();
		}
		createHTableMap();
	}

	public HBaseTableManager(String newConf) {

		hBaseConfigManager = new HBaseConfigurationManager(newConf);
		tableMap = new HashMap<String, HTable>();
		createHTableMap();
	}

	public static void closeActiveConnection() {

		HBaseConfigurationManager.hbaseConf = null;

	}

	private void createHTableMap() {

		try {

			// HBaseTableList hbaseTables[] = HBaseTableList.values();
			// int hTablesCount = HBaseTableList.values().length;
			// for(int tableIterator = 0; tableIterator < hTablesCount;
			// tableIterator ++){
			tableMap.put("LatestId", new HTable(HBaseConfigurationManager.getHbaseConf(), "LatestId"));

			// }
		}
		catch (IOException e) {
		}
	}

	private static void addHTableIntoMap(String tableName) {

		try {
			tableMap.put(tableName, new HTable(HBaseConfigurationManager.getHbaseConf(), tableName));
		}
		catch (IOException e) {
		}
	}

	public static HTable getTable(String tableName) {

		HTable table = tableMap.get(tableName);

		if (table == null) {
			addHTableIntoMap(tableName);
			table = tableMap.get(tableName);
		}

		return table;
	}

	public static String[] getColFamilies(String tableName) {

		String[] families = null;
		try {
			Set<byte[]> familySet = getTable(tableName).getTableDescriptor().getFamiliesKeys();

			Object[] allFamiles = familySet.toArray();
			families = new String[allFamiles.length];
			int i = 0;
			for (Object family2 : allFamiles) {
				byte[] family = (byte[]) family2;
				// userTable.getTableDescriptor().getFamily(family).getMaxVersions();
				families[i] = Bytes.toString(family);
				i++;
			}

		}
		catch (IOException e) {
			System.out.println("Exception :" + e);
		}

		return families;
	}

	public static void deleteTableColoumnQualifier(String tableName, String rowName, String qualifierName, String family) {

		Delete del = new Delete(Bytes.toBytes(rowName));

		try {
			del = del.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(qualifierName));
			getTable(tableName).delete(del);
			System.out.println("Coloumn " + rowName + ":" + qualifierName + " deleted");
		}
		catch (IOException e) {
			System.out.println("Exception deleting coloumn Qualifier");
		}

	}

	public static String deleteTableRow(byte[] rowName, String tableName) {

		String result = null;
		try {

			System.out.println("Deleting row: " + Bytes.toString(rowName));
			Delete delRowData = new Delete(rowName);

			getTable(tableName).delete(delRowData);

		}
		catch (IOException e) {
			System.out.println("Exception occured in retrieving data");
		}
		return result;

	}

	/*
	 * public static String[] getColFamilyVersions(HTable userTable) {
	 * 
	 * String[] families = null; try { Set<byte[]> familySet =
	 * userTable.getTableDescriptor().getFamiliesKeys(); int maxversions;
	 * Object[] allFamiles = familySet.toArray(); families = new
	 * String[allFamiles.length]; int i = 0; for (Object family2 : allFamiles) {
	 * byte[] family = (byte[]) family2; maxversions =
	 * userTable.getTableDescriptor().getFamily(family).getMaxVersions();
	 * families[i] = String.valueOf(maxversions); i++; }
	 * 
	 * } catch (IOException e) { System.out.println("Exception :" + e); }
	 * 
	 * return families; }
	 */

	public static String[] getColFamilyVersions(String tableName) {

		String[] families = null;
		try {
			Set<byte[]> familySet = getTable(tableName).getTableDescriptor().getFamiliesKeys();
			int maxversions;
			Object[] allFamiles = familySet.toArray();
			families = new String[allFamiles.length];
			int i = 0;
			for (Object family2 : allFamiles) {
				byte[] family = (byte[]) family2;
				maxversions = getTable(tableName).getTableDescriptor().getFamily(family).getMaxVersions();
				families[i] = String.valueOf(maxversions);
				i++;
			}

		}
		catch (IOException e) {
			System.out.println("Exception :" + e);
		}

		return families;
	}

	public static String[] getAllTableNames() {

		String[] tblnames = null;
		try {
			HTableDescriptor[] tables = HBaseConfigurationManager.getHbaseAdmin().listTables();
			tblnames = new String[tables.length];
			int i = 0;
			for (HTableDescriptor tbl : tables) {
				tblnames[i] = tbl.getNameAsString();
				i++;
			}
		}
		catch (IOException e) {
			Logger.getLogger(HBaseTableManager.class).log(Level.ERROR, ExceptionUtils.getFullStackTrace(e));
		}

		return tblnames;
	}

	public static boolean getDataFiller(Result resultObject, String colFamily) {

		String[] coloumns = null;
		String[][] data = null;
		String action = "";

		/*
		 * getting the Map of map containing the the column family, qualifier
		 * and content info for each row in the result object
		 */

		if (!resultObject.isEmpty()) {

			NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultMap = resultObject.getNoVersionMap();
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> resultVersionMap = resultObject.getMap();

			byte[] key = Bytes.toBytes(colFamily);

			NavigableMap<byte[], byte[]> innerMap = null;
			innerMap = resultMap.get(key);

			if (innerMap == null) {
				return false;
			}
			coloumns = new String[innerMap.size() + 1];
			data = new String[innerMap.size() + 1][2];

			byte[] innerKey = innerMap.firstKey();
			coloumns[0] = "Row Key";
			data[0][0] = Bytes.toString(resultObject.getRow());
			data[0][1] = "Row Key";
			int i = 1;
			while (innerKey != null) {
				coloumns[i] = Bytes.toString(innerKey);
				/*
				 * if(coloumns[i].equals("id")) {
				 * HBaseManagerViewTable.coloumnTypeList.put("id", "LONG"); }
				 */

				if (HBaseManagerViewTable.coloumnTypeList.containsKey(coloumns[i])) {
					byte[] value = resultObject.getValue(key, innerKey);
					action = HBaseManagerViewTable.coloumnTypeList.get(coloumns[i]);

					System.out.print("Converting to type:" + action);

					data[i][0] = getConvertedValue(value, action, coloumns[i]);
				}
				else {
					data[i][0] = Bytes.toString(resultObject.getValue(key, innerKey));
				}
				data[i][1] = Bytes.toString(innerKey);

				i++;
				innerKey = innerMap.higherKey(innerKey);
			}
		}
		columnQualifiers = coloumns;
		ResultMap = data;
		return true;
	}

	public static KeyValue[] getDataFillerKeyValues(Result resultObject) {

		KeyValue[] values = null;
		if (!resultObject.isEmpty()) {
			values = resultObject.raw();

		}
		return values;
	}

	public static String getConvertedValue(byte[] value, String action, String coloumn) {

		String cnvValue = "";
		try {

			if (action.equals("INTEGER")) {
				cnvValue = String.valueOf(Bytes.toInt(value));
			}
			else if (action.equals("DOUBLE")) {
				cnvValue = String.valueOf(Bytes.toDouble(value));
			}
			else if (action.equals("SHORT")) {
				cnvValue = String.valueOf(Bytes.toShort(value));
			}
			else if (action.equals("LONG")) {
				cnvValue = String.valueOf(Bytes.toLong(value));
			}
			else if (action.equals("BOOLEAN")) {
				cnvValue = String.valueOf(Bytes.toBoolean(value));
			}
			else if (action.equals("DATEANDTIME")) {
				cnvValue = String.valueOf(Bytes.toBoolean(value));
				long val = Bytes.toLong(value);
				cnvValue = String.valueOf(new Date(val));
			}

		}
		catch (Exception e) {
			System.out.println("InConvertible");
			HBaseManagerViewTable.coloumnTypeList.remove(coloumn);
		}
		return cnvValue;

	}

	public static byte[] getConvertedValue(String value, String action, String coloumn) {

		byte[] cnvValue = "".getBytes();

		try {
			if (action.equals("INTEGER")) {
				cnvValue = Bytes.toBytes(Integer.parseInt(value));
			}
			else if (action.equals("DOUBLE")) {
				cnvValue = Bytes.toBytes(Double.parseDouble(value));
			}
			else if (action.equals("SHORT")) {
				cnvValue = Bytes.toBytes(Short.parseShort(value));
			}
			else if (action.equals("LONG")) {
				cnvValue = Bytes.toBytes(Long.parseLong(value));
			}
			else if (action.equals("BOOLEAN")) {
				cnvValue = Bytes.toBytes(Boolean.parseBoolean(value));
			}
			else if (action.equals("DATEANDTIME")) {

				SimpleDateFormat format = new SimpleDateFormat("dd MMM, yyyy HH:mm:ss a");
				Date thisDate = format.parse(value);
				cnvValue = Bytes.toBytes(thisDate.getTime());

			}
		}
		catch (Exception e) {
			System.out.println("InConvertible");
			HBaseManagerViewTable.coloumnTypeList.remove(coloumn);
		}
		return cnvValue;

	}

	public static ResultScanner getAllDataInRangeOfFamily(String startRowRange, String stopRowRange, byte[][] cfs, String ctableName) {

		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.setStartRow(Bytes.toBytes(startRowRange));

		for (byte[] family : cfs) {
			scan.addFamily(family);
		}

		if (stopRowRange != null) {
			scan.setStopRow(Bytes.toBytes(stopRowRange));
		}

		ResultScanner resultScanner = null;

		try {
			resultScanner = getTable(ctableName).getScanner(scan);

		}
		catch (Exception e) {

		}
		return resultScanner;
	}

	public static ResultScanner getList(String startRowRange, String stopRowRange, byte[] cf1, byte[] cf2, long limit, FilterList filterList, String ctableName) {

		Scan scan = new Scan();
		scan.addFamily(cf1);
		scan.setStartRow(Bytes.toBytes(startRowRange));

		if (stopRowRange != null) {
			scan.setStopRow(Bytes.toBytes(stopRowRange));
		}
		if (limit != 0) {

			filterList.addFilter(new PageFilter(limit));
		}
		else {
			filterList.addFilter(new PageFilter(100));
		}
		scan.setFilter(filterList);
		ResultScanner resultScanner = null;

		try {
			resultScanner = getTable(ctableName).getScanner(scan);

		}
		catch (Exception e) {

		}
		return resultScanner;
	}

	public static void insertRowToDb(String[][] tableData, String colFamily, String tableName) {

		String rowKey = tableData[0][0];

		Put resourcePut = new Put(Bytes.toBytes(rowKey));

		String[] userDataList = new String[tableData.length];
		String[] userColList = new String[tableData.length];

		String valueString = null;
		byte[] putValue = null;
		String action = null;

		for (int i = 0; i < tableData.length; i++) {
			userDataList[i] = tableData[i][0];
			userColList[i] = tableData[i][1];
		}

		for (int i = 1; i < userDataList.length; i++) {
			valueString = userDataList[i];
			if (HBaseManagerViewTable.coloumnTypeList.containsKey(userColList[i])) {
				action = HBaseManagerViewTable.coloumnTypeList.get(userColList[i]);
				putValue = HBaseTableManager.getConvertedValue(valueString, action, userColList[i]);
			}
			else {
				putValue = Bytes.toBytes(userDataList[i]);
			}
			resourcePut.add(Bytes.toBytes(colFamily), Bytes.toBytes(userColList[i]), putValue);
		}

		_insert(resourcePut, tableName);
	}

	public static void _insert(Put put, String tableName) {

		// String rowKey = Bytes.toString(put.getRow());

		try {
			getTable(tableName).put(put);
		}
		catch (IOException e) {

		}

	}

	public static void _insert(List<Put> putList, String tableName) {

		// String rowKey = Bytes.toString(put.getRow());

		try {
			getTable(tableName).put(putList);
		}
		catch (IOException e) {

		}

	}

	public static HBaseTableManager getInstance() {

		if (currentTableManager == null) {
			currentTableManager = new HBaseTableManager();
		}
		return currentTableManager;
	}

	public static void resetConnection() {

		currentTableManager = new HBaseTableManager("reset");
	}

	public static void shutdownAliveConnection() {

		if (HBaseConfigurationManager.getHbaseConf() != null)
			HConnectionManager.deleteConnection(HBaseConfigurationManager.getHbaseConf(), true);

		HBaseTableManager.closeActiveConnection();
		HbaseManagerStatic.clearAllConfigs();
	}
}
