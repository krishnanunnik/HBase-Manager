
package com.vg.hbase.operations.base;


public class HbaseManagerTableGetter {

	/*
	 * static HBaseTableManager tblMngr = new HBaseTableManager();
	 * 
	 * public static HBaseTableManager getTblManager() {
	 * 
	 * return tblMngr; }
	 * 
	 * static HTable table;
	 * 
	 * public HbaseManagerTableGetter(String tablename) {
	 * 
	 * table = tblMngr.getTable(tablename); }
	 * 
	 * public HbaseManagerTableGetter() {
	 * 
	 * }
	 * 
	 * public void resetTableManager() {
	 * 
	 * try { tblMngr = new HBaseTableManager(); } catch (Exception e) {
	 * System.out.println("Error while connecting to Hbase: " +
	 * ExceptionUtils.getFullStackTrace(e)); HbaseManagerStatic.SERVER_ERROR =
	 * true; return; } }
	 * 
	 * public static void setTable(String tableName1) {
	 * 
	 * table = tblMngr.getTable(tableName1); }
	 * 
	 * public static HTable getTable(String tableName) {
	 * 
	 * return tblMngr.getTable(tableName);
	 * 
	 * }
	 * 
	 * public String[] getColoumnFamiles(String tableName) {
	 * 
	 * return tblMngr.getColFamilies(tblMngr.getTable(tableName));
	 * 
	 * }
	 * 
	 * public String[] getColoumnFamilyVersions(String tableName) {
	 * 
	 * return tblMngr.getColFamilyVersions(tblMngr.getTable(tableName));
	 * 
	 * }
	 * 
	 * public static HBaseTableManager getTableManagerObject() {
	 * 
	 * return tblMngr;
	 * 
	 * }
	 * 
	 * public String deleteTableRow(byte[] rowName) {
	 * 
	 * String result = null; try {
	 * 
	 * System.out.println("Deleting row: " + Bytes.toString(rowName)); Delete
	 * delRowData = new Delete(rowName);
	 * 
	 * table.delete(delRowData); } catch (IOException e) {
	 * System.out.println("Exception occured in retrieving data"); } return
	 * result;
	 * 
	 * }
	 * 
	 * public void deleteTableColoumnQualifier(String rowName, String
	 * qualifierName, String family) {
	 * 
	 * Delete del = new Delete(Bytes.toBytes(rowName));
	 * 
	 * try { del = del.deleteColumn(Bytes.toBytes(family),
	 * Bytes.toBytes(qualifierName)); table.delete(del);
	 * System.out.println("Coloumn " + rowName + ":" + qualifierName +
	 * " deleted"); } catch (IOException e) {
	 * System.out.println("Exception deleting coloumn Qualifier"); }
	 * 
	 * }
	 * 
	 * public String[] getTableNames() {
	 * 
	 * return HBaseTableManager.getAllTableNames();
	 * 
	 * }
	 * 
	 * public static ResultScanner getAllDataInRangeOfFamily(String
	 * startRowRange, String stopRowRange, byte[][] cfs, String ctableName) {
	 * 
	 * Scan scan = new Scan(); scan.setMaxVersions();
	 * scan.setStartRow(Bytes.toBytes(startRowRange));
	 * 
	 * for (byte[] family : cfs) { scan.addFamily(family); }
	 * 
	 * if (stopRowRange != null) { scan.setStopRow(Bytes.toBytes(stopRowRange));
	 * }
	 * 
	 * ResultScanner resultScanner = null;
	 * 
	 * try { resultScanner = tblMngr.getTable(ctableName).getScanner(scan);
	 * 
	 * } catch (Exception e) {
	 * 
	 * } return resultScanner; }
	 * 
	 * public static ResultScanner getList(String startRowRange, String
	 * stopRowRange, byte[] cf1, byte[] cf2, long limit, FilterList filterList,
	 * String ctableName) {
	 * 
	 * Scan scan = new Scan(); scan.addFamily(cf1);
	 * scan.setStartRow(Bytes.toBytes(startRowRange));
	 * 
	 * if (stopRowRange != null) { scan.setStopRow(Bytes.toBytes(stopRowRange));
	 * } if (limit != 0) {
	 * 
	 * filterList.addFilter(new PageFilter(limit)); } else {
	 * filterList.addFilter(new PageFilter(100)); } scan.setFilter(filterList);
	 * ResultScanner resultScanner = null;
	 * 
	 * try { resultScanner = tblMngr.getTable(ctableName).getScanner(scan);
	 * 
	 * } catch (Exception e) {
	 * 
	 * } return resultScanner; }
	 * 
	 * public void insertRowToDb(String[][] tableData, String colFamily) {
	 * 
	 * String rowKey = tableData[0][0];
	 * 
	 * Put resourcePut = new Put(Bytes.toBytes(rowKey));
	 * 
	 * String[] userDataList = new String[tableData.length]; String[]
	 * userColList = new String[tableData.length];
	 * 
	 * String valueString = null; byte[] putValue = null; String action = null;
	 * 
	 * for (int i = 0; i < tableData.length; i++) { userDataList[i] =
	 * tableData[i][0]; userColList[i] = tableData[i][1]; }
	 * 
	 * for (int i = 1; i < userDataList.length; i++) { valueString =
	 * userDataList[i]; if
	 * (HBaseManagerViewTable.coloumnTypeList.containsKey(userColList[i])) {
	 * action = HBaseManagerViewTable.coloumnTypeList.get(userColList[i]);
	 * putValue = HBaseTableManager.getConvertedValue(valueString, action,
	 * userColList[i]); } else { putValue = Bytes.toBytes(userDataList[i]); }
	 * resourcePut.add(Bytes.toBytes(colFamily), Bytes.toBytes(userColList[i]),
	 * putValue); }
	 * 
	 * _insert(resourcePut); }
	 * 
	 * public void _insert(Put put) {
	 * 
	 * // String rowKey = Bytes.toString(put.getRow());
	 * 
	 * try { table.put(put); } catch (IOException e) {
	 * 
	 * }
	 * 
	 * }
	 * 
	 * public static void _insert(List<Put> putList) {
	 * 
	 * // String rowKey = Bytes.toString(put.getRow());
	 * 
	 * try { table.put(putList); } catch (IOException e) {
	 * 
	 * }
	 * 
	 * }
	 */
}