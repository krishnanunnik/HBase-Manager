/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */
package com.vg.hbase.manager.ui;

/*
 * To change this template, choose Tools | Templates and open the template in
 * the editor.
 */

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.swing.AbstractButton;
import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JOptionPane;
import javax.swing.JTable;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.table.DefaultTableModel;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.vg.hbase.comm.manager.HBaseTableManager;
import com.vg.hbase.operations.base.HbaseManagerStatic;
import com.vg.hbase.operations.base.HbaseManagerTableGetter;

/**
 * 
 * @author skrishnanunni
 */

public class HBaseManagerViewTable extends javax.swing.JFrame {
    
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    /**
     * Creates new form HBaseManagerViewTable
     */
    
    private boolean mannualRange = false;;
    private static boolean PRESSED_SCAN_ONCE = false;
    private boolean PASS_ALL = false;
    private boolean FILTER_SET = false;
    private boolean ALLOW_UPDATE_COUNT = true;
    private boolean PRESSED_BACKWARD_SEEK;
    private boolean PAGINATED_ALREADY = false;
    
    private long totalRowsBrought = 0;
    private long currentRowLimit = 0;
    private static long CURRENT_SPINNER_COUNT = 100;
    
    private int paginationCount = 0;
    
    private List<String> paginateForwardLimits = new ArrayList<String>();
    private List<String> paginateBackWardLimits = new ArrayList<String>();
    private List<String> coloumnList = new ArrayList<String>();
    private List<String> currentColoumnList = new ArrayList<String>();
    
    private String CURRENT_START_RANGE = "0";
    private String CURRENT_STOP_RANGE = "zzz";
    
    private HbaseManagerTableGetter hbaseTableGetter;
    
    public static HashMap<String, String> coloumnTypeList = new HashMap<String, String>();
    private HashMap<String, String[]> filterStrings = new HashMap<String, String[]>();
    
    private HBaseManagerNewConnection newConnectionDialog;
    private HBaseManagerTableDesign newTableDesign;
    private HBaseManagerAdminPanel adminBenchDialog;
    private HbaseDataBackupRestoreDialog dataBackupDialog;
    
    public HBaseManagerViewTable() {
	
	HbaseManagerStatic.SERVER_NOT_CONNECTED = true;
	
	initComponents();
	
	labelLoadingData.setVisible(false);
	
	this.setVisible(true);
	
	newConnectionDialog = new HBaseManagerNewConnection(this, true);
	newConnectionDialog.setLocationRelativeTo(this);
	newConnectionDialog.setVisible(true);
	
	// paginateBackWardLimits.add("0");
	// paginateForwardLimits.add("zzz");
	
	initUI();
	
	rowLimitSpinner.setValue(500);
	
	CURRENT_SPINNER_COUNT = (Integer) rowLimitSpinner.getValue();
	
	newTableDesign = new HBaseManagerTableDesign(this, true);
	newTableDesign.setLocationRelativeTo(this);
	adminBenchDialog = new HBaseManagerAdminPanel(this, true);
	adminBenchDialog.setLocationRelativeTo(this);
	dataBackupDialog = new HbaseDataBackupRestoreDialog(this, true);
	dataBackupDialog.setLocationRelativeTo(this);
	
    }
    
    private void initUI() {
	
	hbaseTableGetter = new HbaseManagerTableGetter();
	
	totalRowsBrought = 0;
	
	if (HbaseManagerStatic.SERVER_NOT_CONNECTED) {
	    disableControls();
	} else {
	    labelLoadingData.setVisible(true);
	    enableControls();
	    addTableList();
	    mannualRange = false;
	    panelInteranlCustomSet.setVisible(false);
	    userTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
	    radioOR.setSelected(true);
	}
	resetPaginators("Initializing UI");
	
    }
    
    private void addTableList() {
	
	toggleCustomScan.setSelected(false);
	clearTogglePane();
	try {
	    DefaultComboBoxModel model = (DefaultComboBoxModel) comboTableList.getModel();
	    model.removeAllElements();
	}
	catch (Exception e) {
	    
	}
	
	String[] tblList = hbaseTableGetter.getTableNames();
	for (String tblListItem : tblList) {
	    comboTableList.addItem(tblListItem);
	}
	labelLoadingData.setVisible(false);
	
    }
    
    private void AddColoumnList(String tblname) {
	this.setTitle(tblname);
	
	String[] familyList = hbaseTableGetter.getColoumnFamiles(tblname);
	
	comboFamilyList.removeAllItems();
	
	for (String tblListItem : familyList) {
	    comboFamilyList.addItem(tblListItem);
	}
	
    }
    
    private int[] getPositionArray(String[] colData, int listSize, DefaultTableModel tm) {
	int posArray[] = new int[colData.length];
	// int position = -1;
	for (int i = 0; i < colData.length; i++) {
	    
	    for (int j = 0; j < listSize; j++) {
		if (colData[i].equals(tm.getColumnName(j))) {
		    posArray[i] = j;
		    break;
		}
	    }
	}
	
	return posArray;
    }
    
    private String[][] getRowData(int row) {
	List<String> dataList = new ArrayList<String>();
	List<String> colList = new ArrayList<String>();
	String data;
	String colname;
	String rowKey;
	String tablename = (String) comboTableList.getSelectedItem();
	String family = (String) comboFamilyList.getSelectedItem();
	for (int i = 0; i < coloumnList.size(); i++) {
	    data = (String) userTable.getValueAt(row, i);
	    if (StringUtils.isEmpty(data)) {
		colname = userTable.getColumnName(i);
		rowKey = (String) userTable.getValueAt(row, 0);
		new HbaseManagerTableGetter(tablename).deleteTableColoumnQualifier(rowKey, colname, family);
		continue;
	    }
	    
	    dataList.add(data);
	    colList.add(userTable.getColumnName(i));
	    
	}
	
	String[][] tbldata = new String[dataList.size()][2];
	
	for (int i = 0; i < dataList.size(); i++) {
	    
	    tbldata[i][0] = dataList.get(i);
	    tbldata[i][1] = colList.get(i);
	    
	}
	
	return tbldata;
	
    }
    
    private void deleteHbaseRow(String rowKey) {
	
	new HbaseManagerTableGetter((String) comboTableList.getSelectedItem()).deleteTableRow(Bytes.toBytes(rowKey));
	
    }
    
    public String[] getCurrentTableColoumns() {
	currentColoumnList.clear();
	int count = userTable.getColumnCount();
	String[] coloumns = new String[count];
	
	for (int i = 0; i < count; i++) {
	    coloumns[i] = userTable.getColumnName(i);
	    currentColoumnList.add(coloumns[i]);
	}
	
	return coloumns;
	
    }
    
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    
    private void initComponents() {
	jPanel3 = new javax.swing.JPanel();
	// jButton1 = new javax.swing.JButton();
	jPanel1 = new javax.swing.JPanel();
	controlPane = new javax.swing.JPanel();
	jLabel1 = new javax.swing.JLabel();
	panelInteranlCustomSet = new javax.swing.JPanel();
	jTabbedPane2 = new javax.swing.JTabbedPane();
	panelInternalCutomize = new javax.swing.JPanel();
	jLabel3 = new javax.swing.JLabel();
	jLabel4 = new javax.swing.JLabel();
	valueStartRow = new javax.swing.JTextField();
	valueStopRow = new javax.swing.JTextField();
	panelInternalSetFilters = new javax.swing.JPanel();
	jScrollPane1 = new javax.swing.JScrollPane();
	listFiltersSet = new javax.swing.JList(new DefaultListModel());
	jLabel5 = new javax.swing.JLabel();
	jLabel6 = new javax.swing.JLabel();
	comboFieldList = new javax.swing.JComboBox();
	comboCompareOpLsit = new javax.swing.JComboBox();
	jLabel7 = new javax.swing.JLabel();
	jLabel8 = new javax.swing.JLabel();
	valueFilterDataColoumn = new javax.swing.JTextField();
	clickAddFilterToList = new javax.swing.JButton();
	jLabel9 = new javax.swing.JLabel();
	radioAND = new javax.swing.JRadioButton();
	radioOR = new javax.swing.JRadioButton();
	clickRemoveFilterFromList = new javax.swing.JButton();
	jLabel2 = new javax.swing.JLabel();
	comboFamilyList = new javax.swing.JComboBox();
	comboTableList = new javax.swing.JComboBox();
	labelLoadingData = new javax.swing.JLabel();
	clickScan = new javax.swing.JButton();
	toggleCustomScan = new javax.swing.JCheckBox();
	jPanel4 = new javax.swing.JPanel();
	jScrollPane2 = new javax.swing.JScrollPane();
	userTable = new javax.swing.JTable();
	clickInsert = new javax.swing.JButton();
	clickSave = new javax.swing.JButton();
	clickNEWCQ = new javax.swing.JButton();
	clickDelete = new javax.swing.JButton();
	clickRefreshTableData = new javax.swing.JButton();
	clickPaginateBackward = new javax.swing.JButton();
	clickPaginateForward = new javax.swing.JButton();
	rowLimitSpinner = new javax.swing.JSpinner();
	jLabel10 = new javax.swing.JLabel();
	clickConvert = new javax.swing.JButton();
	labelTotalRows = new javax.swing.JLabel();
	jMenuBar1 = new javax.swing.JMenuBar();
	menuConnections = new javax.swing.JMenu();
	menuNewConnection = new javax.swing.JMenuItem();
	menuExit = new javax.swing.JMenuItem();
	menuAdministrator = new javax.swing.JMenu();
	menuOpenTableDesigner = new javax.swing.JMenuItem();
	menuOpenAdmin = new javax.swing.JMenuItem();
	menuClickBackUp = new javax.swing.JMenu();
	jMenuItem1 = new javax.swing.JMenuItem();
	// menuRestoreData = new javax.swing.JMenuItem();
	javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
	jPanel3.setLayout(jPanel3Layout);
	jPanel3Layout.setHorizontalGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGap(0, 100, Short.MAX_VALUE));
	jPanel3Layout.setVerticalGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGap(0, 100, Short.MAX_VALUE));
	
	setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
	setTitle("HBase Manager");
	setResizable(false);
	jPanel1.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
	controlPane.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
	jLabel1.setText("Select Table : ");
	jTabbedPane2.setBorder(javax.swing.BorderFactory.createBevelBorder(javax.swing.border.BevelBorder.RAISED));
	jLabel3.setText("Start Range:");
	jLabel4.setText("Stop Range:");
	valueStartRow.getDocument().addDocumentListener(new DocumentListener() {
	    public void changedUpdate(DocumentEvent e) {
		resetPaginators("Custom Start row Change");
	    }
	    
	    public void removeUpdate(DocumentEvent e) {
		resetPaginators("Custom Start row Change");
	    }
	    
	    public void insertUpdate(DocumentEvent e) {
		resetPaginators("Custom Start row Change");
	    }
	});
	valueStartRow.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		valueStartRowActionPerformed(evt);
	    }
	});
	valueStartRow.addInputMethodListener(new java.awt.event.InputMethodListener() {
	    public void caretPositionChanged(java.awt.event.InputMethodEvent evt) {
	    }
	    
	    public void inputMethodTextChanged(java.awt.event.InputMethodEvent evt) {
		valueStartRowInputMethodTextChanged(evt);
	    }
	});
	valueStopRow.getDocument().addDocumentListener(new DocumentListener() {
	    public void changedUpdate(DocumentEvent e) {
		resetPaginators("Custom Stop row Change");
	    }
	    
	    public void removeUpdate(DocumentEvent e) {
		resetPaginators("Custom Stop row Change");
	    }
	    
	    public void insertUpdate(DocumentEvent e) {
		resetPaginators("Custom Stop row Change");
	    }
	});
	javax.swing.GroupLayout panelInternalCutomizeLayout = new javax.swing.GroupLayout(panelInternalCutomize);
	panelInternalCutomize.setLayout(panelInternalCutomizeLayout);
	panelInternalCutomizeLayout.setHorizontalGroup(panelInternalCutomizeLayout.createParallelGroup(
		javax.swing.GroupLayout.Alignment.LEADING).addGroup(
		panelInternalCutomizeLayout
			.createSequentialGroup()
			.addGap(45, 45, 45)
			.addGroup(
				panelInternalCutomizeLayout
					.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
					.addGroup(
						panelInternalCutomizeLayout
							.createSequentialGroup()
							.addComponent(jLabel3)
							.addPreferredGap(
								javax.swing.LayoutStyle.ComponentPlacement.RELATED)
							.addComponent(valueStartRow,
								javax.swing.GroupLayout.PREFERRED_SIZE, 332,
								javax.swing.GroupLayout.PREFERRED_SIZE))
					.addGroup(
						panelInternalCutomizeLayout
							.createSequentialGroup()
							.addComponent(jLabel4)
							.addPreferredGap(
								javax.swing.LayoutStyle.ComponentPlacement.RELATED)
							.addComponent(valueStopRow,
								javax.swing.GroupLayout.PREFERRED_SIZE, 332,
								javax.swing.GroupLayout.PREFERRED_SIZE)))
			.addContainerGap(209, Short.MAX_VALUE)));
	panelInternalCutomizeLayout.setVerticalGroup(panelInternalCutomizeLayout.createParallelGroup(
		javax.swing.GroupLayout.Alignment.LEADING).addGroup(
		panelInternalCutomizeLayout
			.createSequentialGroup()
			.addContainerGap()
			.addGroup(
				panelInternalCutomizeLayout
					.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
					.addComponent(jLabel3)
					.addComponent(valueStartRow, javax.swing.GroupLayout.PREFERRED_SIZE,
						javax.swing.GroupLayout.DEFAULT_SIZE,
						javax.swing.GroupLayout.PREFERRED_SIZE))
			.addGap(44, 44, 44)
			.addGroup(
				panelInternalCutomizeLayout
					.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
					.addComponent(jLabel4)
					.addComponent(valueStopRow, javax.swing.GroupLayout.PREFERRED_SIZE,
						javax.swing.GroupLayout.DEFAULT_SIZE,
						javax.swing.GroupLayout.PREFERRED_SIZE))
			.addContainerGap(49, Short.MAX_VALUE)));
	
	jTabbedPane2.addTab("Custom Scan Range", panelInternalCutomize);
	
	listFiltersSet.setModel(new DefaultListModel());
	jScrollPane1.setViewportView(listFiltersSet);
	
	jLabel5.setText("Set Value Filters Here");
	
	jLabel6.setText("Field :");
	
	comboFieldList.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		comboFieldListActionPerformed(evt);
	    }
	});
	
	comboCompareOpLsit.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "EQUAL", "GREATER", "LESS",
		"GREATER OR EQUAL", "LESSER OR EQUAL", "NOT OP", "NOT EQUAL" }));
	comboCompareOpLsit.setAutoscrolls(true);
	
	jLabel7.setText("Operation:");
	
	jLabel8.setText("Value:");
	
	clickAddFilterToList.setText("Add Filter >>");
	clickAddFilterToList.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickAddFilterToListActionPerformed(evt);
	    }
	});
	
	jLabel9.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
	jLabel9.setText("Filter List");
	
	radioAND.setText("AND Filter");
	radioAND.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioANDActionPerformed(evt);
	    }
	});
	
	radioOR.setText("OR Filter");
	radioOR.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioORActionPerformed(evt);
	    }
	});
	
	clickRemoveFilterFromList.setText("<<Remove Filter");
	clickRemoveFilterFromList.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickRemoveFilterFromListActionPerformed(evt);
	    }
	});
	
	javax.swing.GroupLayout panelInternalSetFiltersLayout = new javax.swing.GroupLayout(panelInternalSetFilters);
	panelInternalSetFilters.setLayout(panelInternalSetFiltersLayout);
	panelInternalSetFiltersLayout
		.setHorizontalGroup(panelInternalSetFiltersLayout
			.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
			.addGroup(
				panelInternalSetFiltersLayout
					.createSequentialGroup()
					.addGroup(
						panelInternalSetFiltersLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
							.addGroup(
								panelInternalSetFiltersLayout
									.createSequentialGroup()
									.addContainerGap()
									.addGroup(
										panelInternalSetFiltersLayout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.TRAILING)
											.addGroup(
												panelInternalSetFiltersLayout
													.createSequentialGroup()
													.addComponent(
														jLabel6)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.RELATED)
													.addComponent(
														comboFieldList,
														javax.swing.GroupLayout.PREFERRED_SIZE,
														javax.swing.GroupLayout.DEFAULT_SIZE,
														javax.swing.GroupLayout.PREFERRED_SIZE)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.RELATED)
													.addComponent(
														jLabel7)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.RELATED)
													.addComponent(
														comboCompareOpLsit,
														javax.swing.GroupLayout.PREFERRED_SIZE,
														113,
														javax.swing.GroupLayout.PREFERRED_SIZE)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
													.addComponent(
														jLabel8)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.RELATED)
													.addComponent(
														valueFilterDataColoumn,
														javax.swing.GroupLayout.PREFERRED_SIZE,
														119,
														javax.swing.GroupLayout.PREFERRED_SIZE)
													.addGap(69, 69,
														69))
											.addGroup(
												panelInternalSetFiltersLayout
													.createSequentialGroup()
													.addComponent(
														radioAND)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
													.addComponent(
														radioOR)
													.addPreferredGap(
														javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
													.addGroup(
														panelInternalSetFiltersLayout
															.createParallelGroup(
																javax.swing.GroupLayout.Alignment.LEADING,
																false)
															.addComponent(
																clickRemoveFilterFromList,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																Short.MAX_VALUE)
															.addComponent(
																clickAddFilterToList,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																javax.swing.GroupLayout.DEFAULT_SIZE,
																Short.MAX_VALUE))
													.addGap(20, 20,
														20))))
							.addGroup(
								panelInternalSetFiltersLayout
									.createSequentialGroup()
									.addGap(190, 190, 190)
									.addComponent(jLabel5)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)))
					.addGroup(
						panelInternalSetFiltersLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
							.addComponent(jLabel9, javax.swing.GroupLayout.PREFERRED_SIZE,
								108, javax.swing.GroupLayout.PREFERRED_SIZE)
							.addComponent(jScrollPane1,
								javax.swing.GroupLayout.PREFERRED_SIZE, 93,
								javax.swing.GroupLayout.PREFERRED_SIZE))
					.addContainerGap(68, Short.MAX_VALUE)));
	panelInternalSetFiltersLayout
		.setVerticalGroup(panelInternalSetFiltersLayout
			.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
			.addGroup(
				panelInternalSetFiltersLayout
					.createSequentialGroup()
					.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
					.addGroup(
						panelInternalSetFiltersLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
							.addComponent(jLabel9, javax.swing.GroupLayout.PREFERRED_SIZE,
								14, javax.swing.GroupLayout.PREFERRED_SIZE)
							.addComponent(jLabel5))
					.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
					.addGroup(
						panelInternalSetFiltersLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
							.addGroup(
								panelInternalSetFiltersLayout
									.createSequentialGroup()
									.addGroup(
										panelInternalSetFiltersLayout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.BASELINE)
											.addComponent(jLabel6)
											.addComponent(
												comboFieldList,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE)
											.addComponent(
												comboCompareOpLsit,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE)
											.addComponent(jLabel7)
											.addComponent(jLabel8)
											.addComponent(
												valueFilterDataColoumn,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE))
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addGroup(
										panelInternalSetFiltersLayout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.BASELINE)
											.addComponent(
												clickAddFilterToList)
											.addComponent(radioAND)
											.addComponent(radioOR))
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(clickRemoveFilterFromList))
							.addComponent(jScrollPane1,
								javax.swing.GroupLayout.PREFERRED_SIZE, 97,
								javax.swing.GroupLayout.PREFERRED_SIZE))
					.addGap(25, 25, 25)));
	
	jTabbedPane2.addTab("Custom Filters", panelInternalSetFilters);
	
	javax.swing.GroupLayout panelInteranlCustomSetLayout = new javax.swing.GroupLayout(panelInteranlCustomSet);
	panelInteranlCustomSet.setLayout(panelInteranlCustomSetLayout);
	panelInteranlCustomSetLayout.setHorizontalGroup(panelInteranlCustomSetLayout.createParallelGroup(
		javax.swing.GroupLayout.Alignment.LEADING).addGroup(
		javax.swing.GroupLayout.Alignment.TRAILING,
		panelInteranlCustomSetLayout
			.createSequentialGroup()
			.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
			.addComponent(jTabbedPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 661,
				javax.swing.GroupLayout.PREFERRED_SIZE).addGap(85, 85, 85)));
	panelInteranlCustomSetLayout.setVerticalGroup(panelInteranlCustomSetLayout.createParallelGroup(
		javax.swing.GroupLayout.Alignment.LEADING).addGroup(
		panelInteranlCustomSetLayout
			.createSequentialGroup()
			.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
			.addComponent(jTabbedPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 176,
				javax.swing.GroupLayout.PREFERRED_SIZE)));
	
	jLabel2.setText("Family:");
	
	comboFamilyList.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		comboFamilyListActionPerformed(evt);
	    }
	});
	
	comboTableList.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		comboTableListActionPerformed(evt);
	    }
	});
	
	labelLoadingData.setText("Loading Table Data ...");
	
	clickScan.setText("Scan Now");
	clickScan.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickScanActionPerformed(evt);
	    }
	});
	
	toggleCustomScan.setText("Customize Scan");
	toggleCustomScan.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		toggleCustomScanActionPerformed(evt);
	    }
	});
	
	javax.swing.GroupLayout controlPaneLayout = new javax.swing.GroupLayout(controlPane);
	controlPane.setLayout(controlPaneLayout);
	controlPaneLayout
		.setHorizontalGroup(controlPaneLayout
			.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
			.addGroup(
				controlPaneLayout
					.createSequentialGroup()
					.addContainerGap()
					.addGroup(
						controlPaneLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
							.addGroup(
								controlPaneLayout
									.createSequentialGroup()
									.addComponent(jLabel1)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addGroup(
										controlPaneLayout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.LEADING)
											.addComponent(labelLoadingData)
											.addComponent(
												comboTableList,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												126,
												javax.swing.GroupLayout.PREFERRED_SIZE)))
							.addGroup(
								controlPaneLayout
									.createSequentialGroup()
									.addComponent(jLabel2)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(comboFamilyList,
										javax.swing.GroupLayout.PREFERRED_SIZE,
										126,
										javax.swing.GroupLayout.PREFERRED_SIZE)))
					.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
					.addGroup(
						controlPaneLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
							.addComponent(toggleCustomScan).addComponent(clickScan))
					.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
					.addComponent(panelInteranlCustomSet, javax.swing.GroupLayout.PREFERRED_SIZE,
						674, javax.swing.GroupLayout.PREFERRED_SIZE)
					.addContainerGap(86, Short.MAX_VALUE)));
	controlPaneLayout
		.setVerticalGroup(controlPaneLayout
			.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
			.addGroup(
				controlPaneLayout
					.createSequentialGroup()
					.addContainerGap()
					.addGroup(
						controlPaneLayout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
							.addComponent(panelInteranlCustomSet,
								javax.swing.GroupLayout.PREFERRED_SIZE, 184,
								javax.swing.GroupLayout.PREFERRED_SIZE)
							.addGroup(
								controlPaneLayout
									.createSequentialGroup()
									.addComponent(labelLoadingData)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addGroup(
										controlPaneLayout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.BASELINE)
											.addComponent(jLabel1)
											.addComponent(
												comboTableList,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE)
											.addComponent(toggleCustomScan))
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addGroup(
										controlPaneLayout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.BASELINE)
											.addComponent(jLabel2)
											.addComponent(
												comboFamilyList,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												javax.swing.GroupLayout.DEFAULT_SIZE,
												javax.swing.GroupLayout.PREFERRED_SIZE)
											.addComponent(clickScan))))
					.addContainerGap(24, Short.MAX_VALUE)));
	
	jPanel4.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
	
	userTable.setBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)));
	userTable.setModel(new javax.swing.table.DefaultTableModel(new Object[][] {
	
	}, new String[] {
	
	}));
	jScrollPane2.setViewportView(userTable);
	
	clickInsert.setText("Insert");
	clickInsert.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickInsertActionPerformed(evt);
	    }
	});
	
	clickSave.setText("Save");
	clickSave.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickSaveActionPerformed(evt);
	    }
	});
	
	clickNEWCQ.setText("New CQ");
	clickNEWCQ.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickNEWCQActionPerformed(evt);
	    }
	});
	
	clickDelete.setText("Delete");
	clickDelete.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickDeleteActionPerformed(evt);
	    }
	});
	
	clickRefreshTableData.setText("Refresh");
	clickRefreshTableData.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickRefreshTableDataActionPerformed(evt);
	    }
	});
	
	clickPaginateBackward.setText("< Previous");
	clickPaginateBackward.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickPaginateBackwardActionPerformed(evt);
	    }
	});
	
	clickPaginateForward.setText("Next >");
	clickPaginateForward.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickPaginateForwardActionPerformed(evt);
	    }
	});
	
	rowLimitSpinner.addChangeListener(new javax.swing.event.ChangeListener() {
	    public void stateChanged(javax.swing.event.ChangeEvent evt) {
		rowLimitSpinnerStateChanged(evt);
	    }
	});
	
	jLabel10.setText("Show Rows ");
	
	clickConvert.setText("Convert");
	clickConvert.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickConvertActionPerformed(evt);
	    }
	});
	
	labelTotalRows.setText("Waiting for Scanner");
	
	javax.swing.GroupLayout jPanel4Layout = new javax.swing.GroupLayout(jPanel4);
	jPanel4.setLayout(jPanel4Layout);
	jPanel4Layout
		.setHorizontalGroup(jPanel4Layout
			.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
			.addGroup(
				jPanel4Layout
					.createSequentialGroup()
					.addContainerGap()
					.addGroup(
						jPanel4Layout
							.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING,
								false)
							.addGroup(
								jPanel4Layout
									.createSequentialGroup()
									.addComponent(clickPaginateBackward)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
									.addComponent(clickPaginateForward,
										javax.swing.GroupLayout.PREFERRED_SIZE,
										75,
										javax.swing.GroupLayout.PREFERRED_SIZE)
									.addGap(37, 37, 37)
									.addComponent(jLabel10)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(rowLimitSpinner,
										javax.swing.GroupLayout.PREFERRED_SIZE,
										93,
										javax.swing.GroupLayout.PREFERRED_SIZE)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED,
										javax.swing.GroupLayout.DEFAULT_SIZE,
										Short.MAX_VALUE)
									.addComponent(clickInsert)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(clickSave)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(clickNEWCQ)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(clickDelete)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(clickRefreshTableData)
									.addPreferredGap(
										javax.swing.LayoutStyle.ComponentPlacement.RELATED)
									.addComponent(clickConvert).addGap(72, 72, 72))
							.addGroup(
								jPanel4Layout
									.createSequentialGroup()
									.addGroup(
										jPanel4Layout
											.createParallelGroup(
												javax.swing.GroupLayout.Alignment.LEADING)
											.addComponent(
												jScrollPane2,
												javax.swing.GroupLayout.PREFERRED_SIZE,
												1059,
												javax.swing.GroupLayout.PREFERRED_SIZE)
											.addGroup(
												jPanel4Layout
													.createSequentialGroup()
													.addGap(10, 10,
														10)
													.addComponent(
														labelTotalRows)))
									.addContainerGap()))));
	jPanel4Layout.setVerticalGroup(jPanel4Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGroup(
			jPanel4Layout
				.createSequentialGroup()
				.addContainerGap()
				.addGroup(
					jPanel4Layout
						.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
						.addComponent(clickSave, javax.swing.GroupLayout.PREFERRED_SIZE, 23,
							javax.swing.GroupLayout.PREFERRED_SIZE)
						.addComponent(clickInsert)
						.addComponent(clickRefreshTableData,
							javax.swing.GroupLayout.PREFERRED_SIZE, 23,
							javax.swing.GroupLayout.PREFERRED_SIZE)
						.addComponent(clickDelete)
						.addComponent(clickNEWCQ, javax.swing.GroupLayout.PREFERRED_SIZE, 23,
							javax.swing.GroupLayout.PREFERRED_SIZE)
						.addComponent(clickPaginateBackward)
						.addComponent(clickPaginateForward)
						.addComponent(rowLimitSpinner, javax.swing.GroupLayout.PREFERRED_SIZE,
							javax.swing.GroupLayout.DEFAULT_SIZE,
							javax.swing.GroupLayout.PREFERRED_SIZE).addComponent(jLabel10)
						.addComponent(clickConvert))
				.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
				.addComponent(jScrollPane2, javax.swing.GroupLayout.DEFAULT_SIZE, 579, Short.MAX_VALUE)
				.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
				.addComponent(labelTotalRows, javax.swing.GroupLayout.PREFERRED_SIZE, 14,
					javax.swing.GroupLayout.PREFERRED_SIZE)));
	
	javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
	jPanel1.setLayout(jPanel1Layout);
	jPanel1Layout.setHorizontalGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGroup(
			jPanel1Layout
				.createSequentialGroup()
				.addContainerGap()
				.addGroup(
					jPanel1Layout
						.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
						.addComponent(controlPane, javax.swing.GroupLayout.PREFERRED_SIZE,
							javax.swing.GroupLayout.DEFAULT_SIZE,
							javax.swing.GroupLayout.PREFERRED_SIZE)
						.addComponent(jPanel4, javax.swing.GroupLayout.PREFERRED_SIZE,
							javax.swing.GroupLayout.DEFAULT_SIZE,
							javax.swing.GroupLayout.PREFERRED_SIZE))
				.addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)));
	jPanel1Layout.setVerticalGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGroup(
			jPanel1Layout
				.createSequentialGroup()
				.addContainerGap()
				.addComponent(controlPane, javax.swing.GroupLayout.PREFERRED_SIZE,
					javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
				.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
				.addComponent(jPanel4, javax.swing.GroupLayout.DEFAULT_SIZE,
					javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE).addContainerGap()));
	menuConnections.setText("Connection");
	menuNewConnection.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_N,
		java.awt.event.InputEvent.ALT_MASK));
	menuNewConnection.setText("New Connection");
	menuNewConnection.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		menuNewConnectionActionPerformed(evt);
	    }
	});
	menuConnections.add(menuNewConnection);
	menuExit.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_X,
		java.awt.event.InputEvent.ALT_MASK));
	menuExit.setText("Exit");
	menuExit.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		menuExitActionPerformed(evt);
	    }
	});
	menuConnections.add(menuExit);
	jMenuBar1.add(menuConnections);
	menuAdministrator.setText("Admin");
	menuOpenTableDesigner.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_H,
		java.awt.event.InputEvent.ALT_MASK | java.awt.event.InputEvent.CTRL_MASK));
	menuOpenTableDesigner.setText("HBase Table Designer");
	menuOpenTableDesigner.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		menuOpenTableDesignerActionPerformed(evt);
	    }
	});
	menuAdministrator.add(menuOpenTableDesigner);
	menuOpenAdmin.setAccelerator(javax.swing.KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_A,
		java.awt.event.InputEvent.ALT_MASK | java.awt.event.InputEvent.CTRL_MASK));
	menuOpenAdmin.setText("HBase Administrator Bench");
	menuOpenAdmin.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		menuOpenAdminActionPerformed(evt);
	    }
	});
	menuAdministrator.add(menuOpenAdmin);
	jMenuBar1.add(menuAdministrator);
	menuClickBackUp.setText("Backup and Restore");
	jMenuItem1.setText("Backup/Restore");
	jMenuItem1.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		jMenuItem1ActionPerformed(evt);
	    }
	});
	menuClickBackUp.add(jMenuItem1); /*
					  * *
					  * menuRestoreData.setText("Restore Data"
					  * ); *
					  * menuClickBackUp.add(menuRestoreData
					  * );
					  */
	jMenuBar1.add(menuClickBackUp);
	setJMenuBar(jMenuBar1);
	javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
	getContentPane().setLayout(layout);
	layout.setHorizontalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
		layout.createSequentialGroup()
			.addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE,
				javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE).addGap(0, 10, Short.MAX_VALUE)));
	layout.setVerticalGroup(layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
		javax.swing.GroupLayout.Alignment.TRAILING,
		layout.createSequentialGroup()
			.addContainerGap()
			.addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE,
				javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE).addContainerGap()));
	pack();
    }// </editor-fold>
    
    private void comboTableListActionPerformed(java.awt.event.ActionEvent evt) {
	
	toggleCustomScan.setSelected(false);
	clearTogglePane();
	mannualRange = true;
	AddColoumnList((String) comboTableList.getSelectedItem());
	mannualRange = false;
	
	labelLoadingData.setVisible(false);
    }
    
    private void comboFamilyListActionPerformed(java.awt.event.ActionEvent evt) {
	coloumnList.clear();
	if (mannualRange && comboFamilyList.getItemCount() > 0)
	
	{
	    populateColoumnList();
	}
	
	resetPaginators("Family List Changed");
	coloumnTypeList.clear();
    }
    
    private void resetPaginators(String origin) {
	System.out.println("\n Restting Paginators by " + origin);
	
	paginateBackWardLimits = new ArrayList<String>();
	paginateForwardLimits = new ArrayList<String>();
	
	// paginateBackWardLimits.add("0");
	// paginateForwardLimits.add("zzz");
	
	clickPaginateForward.setEnabled(false);
	clickPaginateBackward.setEnabled(false);
	
	PRESSED_SCAN_ONCE = false;
	ALLOW_UPDATE_COUNT = true;
	PAGINATED_ALREADY = false;
	PRESSED_BACKWARD_SEEK = false;
	
	paginationCount = 0;
	totalRowsBrought = 0;
	
	labelTotalRows.setText("Waiting for Scanner");
    }
    
    private void clickAddFilterToListActionPerformed(java.awt.event.ActionEvent evt) {
	
	String colname = (String) comboFieldList.getSelectedItem();
	String operation = (String) comboCompareOpLsit.getSelectedItem();
	String val = valueFilterDataColoumn.getText();
	
	String filterString[] = new String[3];
	filterString[0] = colname;
	filterString[1] = operation;
	filterString[2] = val;
	filterStrings.put(colname, filterString);
	
	String listText = colname + ":" + val;
	
	DefaultListModel lmodel = (DefaultListModel) listFiltersSet.getModel();
	
	lmodel.addElement(listText);
	FILTER_SET = true;
	
	resetPaginators("Filter Added");
	
    }
    
    private void clickSaveActionPerformed(java.awt.event.ActionEvent evt) {
	
	// Insert New Row
	
	int[] selectedRow = userTable.getSelectedRows();
	String rowKey = null;
	String[][] tableData = null;
	for (int i = 0; i < selectedRow.length; i++) {
	    rowKey = (String) userTable.getValueAt(selectedRow[i], 0);
	    if (StringUtils.isNotEmpty(rowKey)) {
		tableData = getRowData(selectedRow[i]);
		new HbaseManagerTableGetter((String) comboTableList.getSelectedItem()).insertRowToDb(tableData,
			(String) comboFamilyList.getSelectedItem());
	    }
	    
	}
	
    }
    
    private void clickInsertActionPerformed(java.awt.event.ActionEvent evt) {
	
	int pos = userTable.getRowCount();
	
	String[] x;
	
	DefaultTableModel tmodel = (DefaultTableModel) userTable.getModel();
	if (pos <= 0) {
	    
	    pos = 0;
	    // String newcolq
	    // =JOptionPane.showInputDialog("Provide atleast one Coloumn Qualifier for Inserting");
	    tmodel.addColumn("Row Key");
	    coloumnList.add("Row Key");
	    userTable.getColumnModel().getColumn(0).setMinWidth(400);
	    clickNEWCQActionPerformed(null);
	    
	}
	
	x = new String[] {};
	tmodel.insertRow(pos, x);
	
	userTable.setAutoscrolls(true);
	userTable.setRowSelectionInterval(pos, pos);
	userTable.setRowSelectionAllowed(true);
    }
    
    private void clickRefreshTableDataActionPerformed(java.awt.event.ActionEvent evt) {
	addTableList();
    }
    
    private void clickDeleteActionPerformed(java.awt.event.ActionEvent evt) {
	
	// userTable.setSelectionMode(ListSelectionModel.MULTIPLE_INTERVAL_SELECTION);
	
	int[] selectedRow = userTable.getSelectedRows();
	
	// JOptionPane.showMessageDialog(this, "Selected Rows = "+
	// selectedRow.length);
	
	DefaultTableModel tm = (DefaultTableModel) userTable.getModel();
	
	String rowKey = null;
	int row = 0;
	
	for (int i = 0; i < selectedRow.length; i++) {
	    row = userTable.getSelectedRow();
	    rowKey = (String) userTable.getValueAt(row, 0);
	    deleteHbaseRow(rowKey);
	    tm.removeRow(row);
	    
	}
	
    }
    
    private void clickNEWCQActionPerformed(java.awt.event.ActionEvent evt) {
	DefaultTableModel tm = (DefaultTableModel) userTable.getModel();
	String str = JOptionPane.showInputDialog(null, "New Coloumn Qualifier Name : ", "HbaseManager : VG", 1);
	tm.addColumn(str);
	coloumnList.add(str);
	
    }
    
    private void toggleCustomScanActionPerformed(java.awt.event.ActionEvent evt) {
	AbstractButton abstractButton = (AbstractButton) evt.getSource();
	boolean selected = abstractButton.getModel().isSelected();
	
	if (selected) {
	    panelInteranlCustomSet.setVisible(true);
	    mannualRange = true;
	    
	    populateColoumnList();
	} else {
	    
	    clearTogglePane();
	}
    }
    
    private void clearTogglePane() {
	
	comboFieldList.removeAllItems();
	valueStartRow.setText("0");
	valueStopRow.setText("zzz");
	valueFilterDataColoumn.setText("");
	DefaultListModel lm = (DefaultListModel) listFiltersSet.getModel();
	lm.clear();
	filterStrings.clear();
	panelInteranlCustomSet.setVisible(false);
	mannualRange = false;
    }
    
    private void clickScanActionPerformed(java.awt.event.ActionEvent evt) {
	
	DefaultTableModel tableModel = (DefaultTableModel) userTable.getModel();
	String tblData[][];
	String colData[];
	String firstRow = "0";
	FilterList newFilterList = null;
	
	System.out.print("Clearing Table");
	
	coloumnList.clear();
	
	currentRowLimit = (Integer) rowLimitSpinner.getValue();
	
	if (currentRowLimit != CURRENT_SPINNER_COUNT) {
	    resetPaginators("Spinner Value Mannual Change");
	    
	    if (mannualRange) {
		CURRENT_START_RANGE = valueStartRow.getText();
		CURRENT_STOP_RANGE = valueStopRow.getText();
	    } else {
		CURRENT_START_RANGE = "0";
		CURRENT_STOP_RANGE = "zz";
	    }
	    
	    CURRENT_SPINNER_COUNT = currentRowLimit;
	}
	
	HBaseTableManager tblmngr = HbaseManagerTableGetter.getTblManager();
	
	String colFamily;
	if (!PAGINATED_ALREADY) {
	    
	    if (mannualRange) {
		
		CURRENT_START_RANGE = valueStartRow.getText();
		CURRENT_STOP_RANGE = valueStopRow.getText();
		
		if (StringUtils.isEmpty(CURRENT_START_RANGE)) {
		    CURRENT_START_RANGE = "0";
		} else if (StringUtils.isEmpty(CURRENT_STOP_RANGE)) {
		    CURRENT_STOP_RANGE = "zz";
		}
		
		CURRENT_SPINNER_COUNT = currentRowLimit;
		
		if (FILTER_SET) {
		    System.out.println("Filter Set, Getting all Filterds");
		    newFilterList = obtainFilterList();
		    System.out.print("Filters; " + newFilterList.getFilters().isEmpty());
		    
		}
		
	    } else {
		CURRENT_START_RANGE = "0";
		CURRENT_STOP_RANGE = "zzz";
	    }
	    
	}
	
	colFamily = (String) comboFamilyList.getSelectedItem();
	String tblName = (String) comboTableList.getSelectedItem();
	if (newFilterList == null) {
	    System.out.print("\nNo Filters Applied");
	    newFilterList = new FilterList();
	}
	
	System.out.println("\nStart@:" + CURRENT_START_RANGE + "\n" + "Stop @: " + CURRENT_STOP_RANGE
		+ "\nRow Limit @: " + currentRowLimit);
	System.out.println("\nPagination Count :" + paginationCount + "\nPaginatorSize :"
		+ paginateBackWardLimits.size() + " , " + paginateForwardLimits.size());
	
	ResultScanner resultScan = HbaseManagerTableGetter.getList(CURRENT_START_RANGE, CURRENT_STOP_RANGE,
		Bytes.toBytes(colFamily), null, currentRowLimit, newFilterList, tblName);
	
	int row = 0;
	
	tableModel.getDataVector().removeAllElements();
	userTable.removeAll();
	
	boolean readFirstRow = false;
	
	if (resultScan == null) {
	    return;
	}
	for (Result resultObj : resultScan) {
	    // Get Table entriy for each row, add cols to list if does not exist
	    
	    firstRow = Bytes.toString(resultObj.getRow());
	    if (!readFirstRow) {
		
		readFirstRow = true;
		paginateBackWardLimits.add(firstRow);
		// String lastRow = firstRow;
		System.out.println("Added Backward Index : " + firstRow);
		
	    }
	    
	    tblmngr.getDataFiller(resultObj, colFamily);
	    tblData = tblmngr.getResultMap();
	    colData = tblmngr.getColumnQualifiers();
	    
	    for (int i = 0; i < colData.length; i++) {
		
		if (!coloumnList.contains(colData[i])) {
		    coloumnList.add(colData[i]);
		    
		}
		
	    }
	    
	    String newData[] = new String[coloumnList.size()];
	    
	    for (int j = 0; j < coloumnList.size(); j++) {
		newData[j] = coloumnList.get(j);
	    }
	    
	    tableModel.setColumnIdentifiers(newData);
	    
	    String setData[] = new String[coloumnList.size()];
	    
	    int posArray[] = getPositionArray(colData, coloumnList.size(), tableModel);
	    
	    for (int i = 0; i < posArray.length; i++) {
		setData[posArray[i]] = tblData[i][0];
	    }
	    
	    tableModel.insertRow(row++, setData);
	    
	}
	paginateForwardLimits.add(firstRow);
	System.out.println("Added Forward Index : " + firstRow);
	System.out.println("Rendered @:" + tableModel.getRowCount() + " Rows , Limit was to @: " + currentRowLimit);
	
	if (tableModel.getRowCount() < currentRowLimit) {
	    System.out.println("Pagination Disabled");
	    
	    clickPaginateForward.setEnabled(false);
	    
	    if (!PAGINATED_ALREADY) {
		totalRowsBrought = (tableModel.getRowCount());
		clickPaginateBackward.setEnabled(false);
		
	    } else {
		if (ALLOW_UPDATE_COUNT) totalRowsBrought += (tableModel.getRowCount());
		
		clickPaginateBackward.setEnabled(true);
	    }
	    ALLOW_UPDATE_COUNT = false;
	    paginationCount--;
	}
	
	else {
	    
	    System.out.println("Pagination Enabled");
	    clickPaginateForward.setEnabled(true);
	    
	}
	
	if (paginationCount < 0) {
	    // String keepForward = paginateForwardLimits.get(0);
	    // String keepBackward = paginateBackWardLimits.get(0);
	    clickPaginateBackward.setEnabled(false);
	    
	    if (PAGINATED_ALREADY) {
		System.out.println("No more Previous records, Restting paginators and Limits");
		
		clickPaginateForward.setEnabled(true);
		paginationCount = -1;
	    }
	    
	}
	
	if (!PRESSED_SCAN_ONCE && ALLOW_UPDATE_COUNT) {
	    PRESSED_SCAN_ONCE = true;
	    totalRowsBrought = (tableModel.getRowCount());
	    
	} else {
	    if (!PRESSED_BACKWARD_SEEK && ALLOW_UPDATE_COUNT) totalRowsBrought += (tableModel.getRowCount());
	}
	currentRowLimit = (Integer) rowLimitSpinner.getValue();
	
	labelTotalRows.setText("Retrieved " + totalRowsBrought + " rows of " + comboTableList.getSelectedItem());
	
	if (tableModel.getRowCount() > 0) {
	    
	    userTable.getColumnModel().getColumn(0).setMinWidth(400);
	    userTable.setShowVerticalLines(true);
	    
	}
	
	else {
	    clickRefreshTableDataActionPerformed(null);
	    
	}
    }
    
    private void comboFieldListActionPerformed(java.awt.event.ActionEvent evt) {
	
    }
    
    private void radioANDActionPerformed(java.awt.event.ActionEvent evt) {
	radioOR.setSelected(false);
	PASS_ALL = true;
    }
    
    private void radioORActionPerformed(java.awt.event.ActionEvent evt) {
	
	radioAND.setSelected(false);
	PASS_ALL = false;
    }
    
    private void clickRemoveFilterFromListActionPerformed(java.awt.event.ActionEvent evt) {
	DefaultListModel lmodel = (DefaultListModel) listFiltersSet.getModel();
	String selVal = (String) listFiltersSet.getSelectedValue();
	selVal = selVal.split(":")[0];
	
	filterStrings.remove(selVal);
	
	resetPaginators("Filter Removed");
	lmodel.remove(listFiltersSet.getSelectedIndex());
	if (lmodel.isEmpty()) {
	    FILTER_SET = false;
	}
	
    }
    
    private void menuExitActionPerformed(java.awt.event.ActionEvent evt) {
	this.dispose();
    }
    
    private void menuNewConnectionActionPerformed(java.awt.event.ActionEvent evt) {
	try {
	    comboFamilyList.removeAll();
	    comboTableList.removeAllItems();
	}
	catch (Exception e) {
	    System.out.println("Event Bubbling Stopped");
	}
	newConnectionDialog.setVisible(true);
	
	initUI();
	
    }
    
    private void jMenuItem1ActionPerformed(java.awt.event.ActionEvent evt) {
	dataBackupDialog.setVisible(true);
    }
    
    private void menuOpenTableDesignerActionPerformed(java.awt.event.ActionEvent evt) {
	newTableDesign.setVisible(true);
	initUI();
    }
    
    private void menuOpenAdminActionPerformed(java.awt.event.ActionEvent evt) {
	adminBenchDialog.setVisible(true);
	initUI();
    }
    
    private void clickPaginateBackwardActionPerformed(java.awt.event.ActionEvent evt) {
	
	clickPaginateForward.setEnabled(true);
	
	PRESSED_BACKWARD_SEEK = true;
	PAGINATED_ALREADY = true;
	CURRENT_START_RANGE = paginateBackWardLimits.get(paginationCount);
	
	if (!mannualRange) CURRENT_STOP_RANGE = "zzz";
	else
	    CURRENT_STOP_RANGE = valueStopRow.getText();
	paginationCount--;
	
	System.out
		.println("Setting Forward: Start @ :" + CURRENT_START_RANGE + " & " + "Stop @: " + CURRENT_STOP_RANGE);
	clickScanActionPerformed(evt);
	
    }
    
    private void clickPaginateForwardActionPerformed(java.awt.event.ActionEvent evt) {
	
	if (paginationCount == -1) {
	    paginationCount = 0;
	    
	    PAGINATED_ALREADY = true;
	    
	    CURRENT_START_RANGE = paginateForwardLimits.get(0);
	    if (!mannualRange) CURRENT_STOP_RANGE = "zzz";
	    else
		CURRENT_STOP_RANGE = valueStopRow.getText();
	    
	    System.out.println("Setting Forward: Start @ :" + CURRENT_START_RANGE + " & " + "Stop @: "
		    + CURRENT_STOP_RANGE);
	    
	    clickScanActionPerformed(evt);
	    return;
	}
	PAGINATED_ALREADY = true;
	clickPaginateBackward.setEnabled(true);
	CURRENT_START_RANGE = paginateForwardLimits.get(paginationCount);
	if (!mannualRange) CURRENT_STOP_RANGE = "zzz";
	else
	    CURRENT_STOP_RANGE = valueStopRow.getText();
	paginationCount++;
	System.out
		.println("Setting Forward: Start @ :" + CURRENT_START_RANGE + " & " + "Stop @: " + CURRENT_STOP_RANGE);
	clickScanActionPerformed(evt);
	
    }
    
    private void valueStartRowActionPerformed(java.awt.event.ActionEvent evt) {
	
    }
    
    private void valueStartRowInputMethodTextChanged(java.awt.event.InputMethodEvent evt) {
	
    }
    
    private void rowLimitSpinnerStateChanged(javax.swing.event.ChangeEvent evt) {
	resetPaginators("Spinner Limit Changed");
    }
    
    private void clickConvertActionPerformed(java.awt.event.ActionEvent evt) {
	
	ConvertColoumnsDialog cdialog = new ConvertColoumnsDialog(this, true);
	cdialog.setLocationRelativeTo(this);
	// cdialog.show(true);
	cdialog.setVisible(true);
	
    }
    
    public void convertColoumnstoUserData(HashMap<String, String> actionMap) {
	int noColoumnstoConvert = actionMap.size();
	System.out.println("Need to Convert " + noColoumnstoConvert + "to User Types");
	String[] keyList = new String[noColoumnstoConvert];
	actionMap.keySet().toArray(keyList);
	DefaultTableModel tableModel = (DefaultTableModel) userTable.getModel();
	int colIndex = -1;
	int avRows = 0;
	String currentVal;
	// byte[] byteVal;
	String convertedVal;
	String action;
	
	List<String> reloadType = new ArrayList<String>();
	reloadType.add("INTEGER");
	reloadType.add("DOUBLE");
	reloadType.add("LONG");
	reloadType.add("SHORT");
	reloadType.add("BOOLEAN");
	
	try {
	    
	    for (int i = 0; i < noColoumnstoConvert; i++) {
		// get col_name and Index
		colIndex = currentColoumnList.indexOf(keyList[i]);
		action = actionMap.get(keyList[i]);
		coloumnTypeList.put(keyList[i], action);
		avRows = userTable.getRowCount();
		coloumnTypeList.put(keyList[i], action);
		if (reloadType.contains(action)) {
		    
		    clickScanActionPerformed(null);
		} else {
		    
		    for (int j = 0; j < avRows; j++) {
			currentVal = (String) userTable.getValueAt(j, colIndex);
			convertedVal = getConvertedValue(currentVal, action);
			tableModel.setValueAt(convertedVal, j, colIndex);
			// coloumnTypeList.put(keyList[i], action);
			
		    }
		}
		
	    }
	}
	catch (Exception e) {
	    
	}
    }
    
    /**
     * @param args
     *            the command line arguments
     */
    public static void main(String args[]) {
	/*
	 * Set the Nimbus look and feel
	 */
	// <editor-fold defaultstate="collapsed"
	// desc=" Look and feel setting code (optional) ">
	/*
	 * If Nimbus (introduced in Java SE 6) is not available, stay with the
	 * default look and feel. For details see
	 * http://download.oracle.com/javase
	 * /tutorial/uiswing/lookandfeel/plaf.html
	 */
	try {
	    for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
		if ("Nimbus".equals(info.getName())) {
		    javax.swing.UIManager.setLookAndFeel(info.getClassName());
		    break;
		}
	    }
	}
	catch (ClassNotFoundException ex) {
	    java.util.logging.Logger.getLogger(HBaseManagerViewTable.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	catch (InstantiationException ex) {
	    java.util.logging.Logger.getLogger(HBaseManagerViewTable.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	catch (IllegalAccessException ex) {
	    java.util.logging.Logger.getLogger(HBaseManagerViewTable.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	catch (javax.swing.UnsupportedLookAndFeelException ex) {
	    java.util.logging.Logger.getLogger(HBaseManagerViewTable.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	// </editor-fold>
	
	/*
	 * Create and display the form
	 */
	java.awt.EventQueue.invokeLater(new Runnable() {
	    
	    public void run() {
		new HBaseManagerViewTable().setVisible(true);
	    }
	});
    }
    
    // Variables declaration - do not modify
    private javax.swing.JButton clickAddFilterToList;
    private javax.swing.JButton clickConvert;
    private javax.swing.JButton clickDelete;
    private javax.swing.JButton clickInsert;
    private javax.swing.JButton clickNEWCQ;
    private javax.swing.JMenuItem jMenuItem1;
    private javax.swing.JButton clickPaginateForward;
    private javax.swing.JButton clickPaginateBackward;
    private javax.swing.JButton clickRefreshTableData;
    private javax.swing.JButton clickRemoveFilterFromList;
    private javax.swing.JButton clickSave;
    private javax.swing.JButton clickScan;
    private javax.swing.JComboBox comboCompareOpLsit;
    private javax.swing.JComboBox comboFamilyList;
    private javax.swing.JComboBox comboFieldList;
    private javax.swing.JMenu menuClickBackUp;
    private javax.swing.JComboBox comboTableList;
    private javax.swing.JPanel controlPane;
    
    // private javax.swing.JButton jButton1;;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel10;
    private javax.swing.JLabel jLabel2;
    // private javax.swing.JMenuItem menuRestoreData;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JLabel jLabel7;
    private javax.swing.JLabel jLabel8;
    private javax.swing.JLabel jLabel9;
    private javax.swing.JMenuBar jMenuBar1;
    
    private javax.swing.JSpinner rowLimitSpinner;
    
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JPanel jPanel4;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JTabbedPane jTabbedPane2;
    private javax.swing.JLabel labelLoadingData;
    private javax.swing.JList listFiltersSet;
    private javax.swing.JMenu menuAdministrator;
    private javax.swing.JLabel labelTotalRows;
    
    private javax.swing.JMenu menuConnections;
    private javax.swing.JMenuItem menuExit;
    private javax.swing.JMenuItem menuNewConnection;
    private javax.swing.JMenuItem menuOpenAdmin;
    private javax.swing.JMenuItem menuOpenTableDesigner;
    
    private javax.swing.JPanel panelInteranlCustomSet;
    private javax.swing.JPanel panelInternalCutomize;
    private javax.swing.JPanel panelInternalSetFilters;
    private javax.swing.JRadioButton radioAND;
    private javax.swing.JRadioButton radioOR;
    
    private javax.swing.JCheckBox toggleCustomScan;
    private javax.swing.JTable userTable;
    private javax.swing.JTextField valueFilterDataColoumn;
    private javax.swing.JTextField valueStartRow;
    private javax.swing.JTextField valueStopRow;
    
    // End of variables declaration
    
    private void populateColoumnList() {
	
	comboFieldList.removeAllItems();
	listFiltersSet.removeAll();
	filterStrings.clear();
	
	String colFamily = (String) comboFamilyList.getSelectedItem();
	String tblName = (String) comboTableList.getSelectedItem();
	HBaseTableManager tblmngr = HbaseManagerTableGetter.getTblManager();
	ResultScanner resultScan = HbaseManagerTableGetter.getList("0", "zzz", Bytes.toBytes(colFamily), null,
		currentRowLimit, new FilterList(), tblName);
	
	// int row = 0;
	String colData[];
	
	if (resultScan == null) {
	    
	}
	
	for (Result resultObj : resultScan) {
	    // Get Table entriy for each row, add cols to list if does not exist
	    
	    if (resultObj.isEmpty()) break;
	    
	    tblmngr.getDataFiller(resultObj, colFamily);
	    
	    colData = tblmngr.getColumnQualifiers();
	    
	    for (int i = 0; i < colData.length; i++) {
		
		if (!coloumnList.contains(colData[i])) {
		    coloumnList.add(colData[i]);
		    
		}
		
	    }
	    
	}
	for (int i = 1; i < coloumnList.size(); i++) {
	    comboFieldList.addItem(coloumnList.get(i));
	    
	}
    }
    
    private FilterList obtainFilterList() {
	FilterList filters = null;
	if (PASS_ALL) {
	    
	    filters = new FilterList(Operator.MUST_PASS_ALL);
	    System.out.print("Init:  AND filter");
	} else {
	    filters = new FilterList();
	    System.out.print("Init:  OR filter");
	}
	
	String val;
	String[] vals = new String[3];
	
	byte[] family = Bytes.toBytes((String) comboFamilyList.getSelectedItem());
	
	SingleColumnValueFilter valFilter = null;
	
	System.out.print("ListSize :" + listFiltersSet.getModel().getSize());
	for (int i = 0; i < listFiltersSet.getModel().getSize(); i++) {
	    val = (String) listFiltersSet.getModel().getElementAt(i);
	    val = val.split(":")[0];
	    vals = filterStrings.get(val);
	    
	    System.out.println("Filter String:" + val + "Searching:" + vals[1] + " for :" + vals[2]);
	    // "EQUAL", "GREATER", "LESS", "GREATER OR EQUAL",
	    // "LESSER OR EQUAL", "NOT OP", "NOT EQUAL"
	    if (vals[1].equals("EQUAL")) {
		
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.EQUAL,
			Bytes.toBytes(vals[2]));
		valFilter.setFilterIfMissing(true);
		filters.addFilter(valFilter);
		System.out.println("Appending Filter EQUAL");
		
	    } else if (vals[1].equals("NOT EQUAL")) {
		
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.NOT_EQUAL,
			Bytes.toBytes(vals[2]));
		valFilter.setFilterIfMissing(true);
		filters.addFilter(valFilter);
		System.out.println("Appending Filter NOT EQUAL");
		
	    } else if (vals[1].equals("GREATER")) {
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.GREATER,
			Bytes.toBytes(vals[2]));
		filters.addFilter(valFilter);
		System.out.println("Appending Filter GREATER");
		
	    } else if (vals[1].equals("LESS")) {
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.LESS,
			Bytes.toBytes(vals[2]));
		filters.addFilter(valFilter);
		System.out.println("Appending Filter LESS");
		
	    } else if (vals[1].equals("GREATER OR EQUAL")) {
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.GREATER_OR_EQUAL,
			Bytes.toBytes(vals[2]));
		filters.addFilter(valFilter);
		System.out.println("Appending Filter GREATER OR EQUAL");
		
	    }
	    if (vals[1].equals("LESSER OR EQUAL")) {
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.LESS_OR_EQUAL,
			Bytes.toBytes(vals[2]));
		filters.addFilter(valFilter);
		System.out.println("Appending Filter LESSER OR EQUAL");
		
	    } else if (vals[1].equals("NOT OP")) {
		valFilter = new SingleColumnValueFilter(family, Bytes.toBytes(vals[0]), CompareOp.NO_OP,
			Bytes.toBytes(vals[2]));
		filters.addFilter(valFilter);
		System.out.println("Appending Filter NOT OP");
		
	    } else {
		continue;
	    }
	    
	}
	
	return filters;
	
    }
    
    private void disableControls() {
	comboFamilyList.setEnabled(false);
	comboTableList.setEnabled(false);
	clickDelete.setEnabled(false);
	clickInsert.setEnabled(false);
	clickNEWCQ.setEnabled(false);
	clickRefreshTableData.setEnabled(false);
	clickSave.setEnabled(false);
	clickScan.setEnabled(false);
	toggleCustomScan.setEnabled(false);
	panelInteranlCustomSet.setVisible(false);
	userTable.setEnabled(false);
	
    }
    
    private void enableControls() {
	comboFamilyList.setEnabled(true);
	comboTableList.setEnabled(true);
	clickDelete.setEnabled(true);
	clickInsert.setEnabled(true);
	clickNEWCQ.setEnabled(true);
	clickRefreshTableData.setEnabled(true);
	clickSave.setEnabled(true);
	clickScan.setEnabled(true);
	toggleCustomScan.setEnabled(true);
	panelInteranlCustomSet.setVisible(true);
	userTable.setEnabled(true);
	
    }
    
    @SuppressWarnings("deprecation")
    private String getConvertedValue(String currentVal, String action) {
	
	byte[] value = Bytes.toBytes(currentVal);
	
	String convertedValue = "";
	if (action.equals("DATE")) {
	    
	    convertedValue = new Date(Long.parseLong(currentVal)).toLocaleString();
	    
	} else if (action.equals("SHORT")) {
	    
	    convertedValue = String.valueOf(Bytes.toShort(value));
	    
	} else if (action.equals("LONG")) {
	    convertedValue = String.valueOf(Bytes.toLong(value));
	} else if (action.equals("BOOLEAN")) {
	    convertedValue = String.valueOf(Bytes.toBoolean(value));
	} else if (action.equals("INTEGER")) {
	    convertedValue = String.valueOf(Bytes.toInt(value));
	    
	} else if (action.equals("DOUBLE")) {
	    convertedValue = String.valueOf(Bytes.toDouble(value));
	    
	} else if (action.equals("TIMESTAMP")) {
	    Long ts = new Date(currentVal).getTime();
	    convertedValue = String.valueOf(ts);
	    
	}
	
	return convertedValue;
    }
}
