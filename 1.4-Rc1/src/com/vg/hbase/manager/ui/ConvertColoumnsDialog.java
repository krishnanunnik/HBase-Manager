package com.vg.hbase.manager.ui;

import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.KeyEvent;
import java.util.HashMap;

import javax.swing.DefaultListModel;

/**
 * 
 * @author skrishnanunni
 */
@SuppressWarnings("serial")
public class ConvertColoumnsDialog extends javax.swing.JDialog {
    
    /**
     * Creates new form ConvertColoumnsDialog
     */
    private DefaultListModel listSourceModel;
    private DefaultListModel listTargetModel;
    
    public static String[] coloumnList;
    
    private HashMap<String, String> actionMap = new HashMap<String, String>();
    HBaseManagerViewTable parentObject;
    
    public ConvertColoumnsDialog(HBaseManagerViewTable parent, boolean modal) {
	super(parent, modal);
	parentObject = parent;
	initComponents();
	
	buttonGroup1.add(radioDate);
	buttonGroup1.add(radioBoolean);
	buttonGroup1.add(radioDouble);
	buttonGroup1.add(radioInteger);
	buttonGroup1.add(radioLong);
	buttonGroup1.add(radioShort);
	buttonGroup1.add(radioTimeStamp);
	buttonGroup1.add(radioBytes);
	listSourceModel = (DefaultListModel) listSource.getModel();
	listTargetModel = (DefaultListModel) listTarget.getModel();
	coloumnList = parent.getCurrentTableColoumns();
	
	populateSourceList();
	
    }
    
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    
    // <editor-fold defaultstate="collapsed" desc="Generated Code">
    private void initComponents() {
	
	buttonGroup1 = new javax.swing.ButtonGroup();
	jScrollPane1 = new javax.swing.JScrollPane();
	listTarget = new javax.swing.JList();
	radioInteger = new javax.swing.JRadioButton();
	radioLong = new javax.swing.JRadioButton();
	radioDate = new javax.swing.JRadioButton();
	radioBoolean = new javax.swing.JRadioButton();
	radioDouble = new javax.swing.JRadioButton();
	radioShort = new javax.swing.JRadioButton();
	jScrollPane2 = new javax.swing.JScrollPane();
	listSource = new javax.swing.JList(new DefaultListModel());
	clickConvert = new javax.swing.JButton();
	jButton2 = new javax.swing.JButton();
	radioTimeStamp = new javax.swing.JRadioButton();
	radioBytes = new javax.swing.JRadioButton();
	
	setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
	setTitle("Convert Coloumn Data");
	setAlwaysOnTop(true);
	setResizable(false);
	Image image = Toolkit.getDefaultToolkit().getImage("hb.png");
	setIconImage(image);
	listTarget.setModel(new DefaultListModel());
	listTarget.addKeyListener(new java.awt.event.KeyAdapter() {
	    public void keyPressed(java.awt.event.KeyEvent evt) {
		listTargetKeyPressed(evt);
	    }
	});
	jScrollPane1.setViewportView(listTarget);
	
	radioInteger.setText("Integer");
	radioInteger.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioIntegerActionPerformed(evt);
	    }
	});
	
	radioLong.setText("Long");
	radioLong.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioLongActionPerformed(evt);
	    }
	});
	
	radioDate.setText("Date | Time");
	radioDate.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioDateActionPerformed(evt);
	    }
	});
	
	radioBoolean.setText("Boolean");
	radioBoolean.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioBooleanActionPerformed(evt);
	    }
	});
	
	radioDouble.setText("Double");
	radioDouble.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioDoubleActionPerformed(evt);
	    }
	});
	
	radioShort.setText("Short");
	radioShort.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioShortActionPerformed(evt);
	    }
	});
	
	listSource.setModel(new DefaultListModel());
	jScrollPane2.setViewportView(listSource);
	
	clickConvert.setText("Convert");
	clickConvert.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		clickConvertActionPerformed(evt);
	    }
	});
	
	jButton2.setText("Cancel");
	jButton2.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		jButton2ActionPerformed(evt);
	    }
	});
	
	radioTimeStamp.setText("Time Stamp");
	radioTimeStamp.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioTimeStampActionPerformed(evt);
	    }
	});
	
	radioBytes.setText("Bytes");
	radioBytes.addActionListener(new java.awt.event.ActionListener() {
	    public void actionPerformed(java.awt.event.ActionEvent evt) {
		radioBytesActionPerformed(evt);
	    }
	});
	
	javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
	getContentPane().setLayout(layout);
	layout.setHorizontalGroup(layout
		.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGroup(
			layout.createSequentialGroup()
				.addGroup(
					layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
						.addGroup(
							layout.createSequentialGroup()
								.addGap(152, 152, 152)
								.addGroup(
									layout.createParallelGroup(
										javax.swing.GroupLayout.Alignment.LEADING)
										.addComponent(radioShort)
										.addComponent(radioLong)
										.addComponent(radioDate)
										.addComponent(radioInteger)
										.addComponent(radioTimeStamp)
										.addComponent(radioDouble)
										.addComponent(radioBoolean)
										.addComponent(radioBytes))
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(jScrollPane1,
									javax.swing.GroupLayout.PREFERRED_SIZE, 124,
									javax.swing.GroupLayout.PREFERRED_SIZE))
						.addGroup(
							layout.createSequentialGroup()
								.addGap(126, 126, 126)
								.addComponent(clickConvert)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.RELATED)
								.addComponent(jButton2)))
				.addContainerGap(42, Short.MAX_VALUE))
		.addGroup(
			layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				layout.createSequentialGroup()
					.addGap(20, 20, 20)
					.addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 124,
						javax.swing.GroupLayout.PREFERRED_SIZE)
					.addContainerGap(261, Short.MAX_VALUE))));
	layout.setVerticalGroup(layout
		.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
		.addGroup(
			layout.createSequentialGroup()
				.addGroup(
					layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
						.addGroup(
							layout.createSequentialGroup()
								.addGap(22, 22, 22)
								.addComponent(jScrollPane1,
									javax.swing.GroupLayout.PREFERRED_SIZE, 228,
									javax.swing.GroupLayout.PREFERRED_SIZE))
						.addGroup(
							layout.createSequentialGroup()
								.addGap(37, 37, 37)
								.addComponent(radioDate)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(radioTimeStamp)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(radioInteger)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(radioLong)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(radioShort)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(radioBoolean)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
								.addComponent(radioDouble)
								.addPreferredGap(
									javax.swing.LayoutStyle.ComponentPlacement.RELATED)
								.addComponent(radioBytes)))
				.addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, 33,
					Short.MAX_VALUE)
				.addGroup(
					layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
						.addComponent(clickConvert).addComponent(jButton2)).addContainerGap())
		.addGroup(
			layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING).addGroup(
				layout.createSequentialGroup()
					.addGap(21, 21, 21)
					.addComponent(jScrollPane2, javax.swing.GroupLayout.PREFERRED_SIZE, 228,
						javax.swing.GroupLayout.PREFERRED_SIZE)
					.addContainerGap(68, Short.MAX_VALUE))));
	
	pack();
    }// </editor-fold>
    
    private void radioDateActionPerformed(java.awt.event.ActionEvent evt) {
	
	setTargetList("DATE");
	
    }
    
    private void radioShortActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("SHORT");        // TODO add your handling code here:
    }
    
    private void radioDoubleActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("DOUBLE");
    }
    
    private void jButton2ActionPerformed(java.awt.event.ActionEvent evt) {
	this.dispose();
    }
    
    private void radioIntegerActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("INTEGER");
    }
    
    private void radioLongActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("LONG");        // TODO add your handling code here:
    }
    
    private void radioBooleanActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("BOOLEAN");        // TODO add your handling code here:
    }
    
    private void listTargetKeyPressed(java.awt.event.KeyEvent evt) {
	if (evt.getKeyCode() == KeyEvent.VK_DELETE) {
	    removeItemsFromSelected();
	}
    }
    
    private void clickConvertActionPerformed(java.awt.event.ActionEvent evt) {
	
	parentObject.convertColoumnstoUserData(actionMap);
    }
    
    private void radioTimeStampActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("TIMESTAMP");        // TODO add your handling code here:
    }
    
    private void radioBytesActionPerformed(java.awt.event.ActionEvent evt) {
	setTargetList("BYTES");        // TODO add your handling code here:
    }
    
    private void setTargetList(String type) {
	
	Object[] selitems = listSource.getSelectedValues();
	int numSelected = selitems.length;
	System.out.println("Items to Convert :" + numSelected);
	String item = null;
	
	for (int i = 0; i < numSelected; i++) {
	    item = (String) selitems[i];
	    
	    actionMap.put(item, type);
	    System.out.println("Putting @: " + item);
	    
	    if (!listTargetModel.contains(item))
	    
	    listTargetModel.addElement(item);
	    
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
	    java.util.logging.Logger.getLogger(ConvertColoumnsDialog.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	catch (InstantiationException ex) {
	    java.util.logging.Logger.getLogger(ConvertColoumnsDialog.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	catch (IllegalAccessException ex) {
	    java.util.logging.Logger.getLogger(ConvertColoumnsDialog.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	catch (javax.swing.UnsupportedLookAndFeelException ex) {
	    java.util.logging.Logger.getLogger(ConvertColoumnsDialog.class.getName()).log(
		    java.util.logging.Level.SEVERE, null, ex);
	}
	// </editor-fold>
	
	/*
	 * Create and display the dialog
	 */
	java.awt.EventQueue.invokeLater(new Runnable() {
	    
	    public void run() {
		ConvertColoumnsDialog dialog = new ConvertColoumnsDialog(new HBaseManagerViewTable(), true);
		dialog.addWindowListener(new java.awt.event.WindowAdapter() {
		    
		    @Override
		    public void windowClosing(java.awt.event.WindowEvent e) {
			System.exit(0);
		    }
		});
		dialog.setVisible(true);
	    }
	});
    }
    
    // Variables declaration - do not modify
    private javax.swing.ButtonGroup buttonGroup1;
    private javax.swing.JButton clickConvert;
    private javax.swing.JButton jButton2;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JScrollPane jScrollPane2;
    private javax.swing.JList listSource;
    private javax.swing.JList listTarget;
    private javax.swing.JRadioButton radioBoolean;
    private javax.swing.JRadioButton radioBytes;
    private javax.swing.JRadioButton radioDate;
    private javax.swing.JRadioButton radioDouble;
    private javax.swing.JRadioButton radioInteger;
    private javax.swing.JRadioButton radioLong;
    private javax.swing.JRadioButton radioShort;
    private javax.swing.JRadioButton radioTimeStamp;
    
    // End of variables declaration
    
    private void populateSourceList() {
	if (coloumnList.length > 0) {
	    for (int i = 1; i < coloumnList.length; i++) {
		listSourceModel.addElement(coloumnList[i]);
	    }
	} else {
	    this.dispose();
	}
	
    }
    
    private void removeItemsFromSelected() {
	
	int[] sellist = listTarget.getSelectedIndices();
	
	System.out.print("Items to remove  : " + sellist.length);
	
	String val = null;
	
	for (int i = 0; i < sellist.length; i++) {
	    val = (String) listTarget.getSelectedValue();
	    System.out.println("Removing @: " + val);
	    actionMap.remove(val);
	    listTargetModel.remove(listTarget.getSelectedIndex());
	}
    }
}
