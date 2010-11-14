package com.bizosys.hsearch.hbase;


import java.awt.AWTException;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.HQuorumPeer;

public class HSearchLauncher {

	static TrayIcon trayIcon;

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		if ( ! SystemTray.isSupported() ) throw new Exception ("Tray Icon is not supported.");
		
	    SystemTray tray = SystemTray.getSystemTray();
	    Image image = Toolkit.getDefaultToolkit().getImage("favicon.gif");

	    ActionListener exitListener = new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	new OHBase("stop", trayIcon).start();
	        	try {
	        		Thread.sleep(1000);
	        	} catch (Exception ex) {
	        	}
	        	new OZookeeper("stop", trayIcon).start();
	            System.exit(0);
	        }
	    };
		    
	    ActionListener startListener = new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	new OHBase("start", trayIcon).start();
	        	try {Thread.sleep(3000);} catch (Exception ex) {}
	        	System.out.println("HBase Server started");
	        }
	    };	
	    
	    ActionListener stopListener = new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	new OHBase("stop", trayIcon).start();
	        	try {
	        		Thread.sleep(1000);
	        	} catch (Exception ex) {
	        	}
	        	//new OZookeeper("stop", trayIcon).start();
	        }
	    };		    
		    
	    PopupMenu popup = new PopupMenu();
		            
	    MenuItem startItem = new MenuItem("Start HBase");
	    startItem.addActionListener(startListener);
	    popup.add(startItem);
		    
	    MenuItem stopItem = new MenuItem("Stop HBase");
	    stopItem.addActionListener(stopListener);
	    popup.add(stopItem);
		    
		    
	    MenuItem exitItem = new MenuItem("Exit");
	    exitItem.addActionListener(exitListener);
	    popup.add(exitItem);

	    trayIcon = new TrayIcon(image, "hsearch", popup);
	    trayIcon.setImageAutoSize(true);

	    try {
	        tray.add(trayIcon);
	    } catch (AWTException e) {
	        System.err.println("TrayIcon could not be added.");
	    }
	}
	
	/**
	 * Starting the zookeeper
	 * @author karan
	 *
	 */
	static class OZookeeper extends Thread {
	    String command = "start";
	    TrayIcon trayIcon = null;
	    
		public OZookeeper(String command, TrayIcon trayIcon) {
	    	this.command = command;
	    	this.trayIcon = trayIcon;
	    }
		
	    public void run() {
	    	try {
	    		HQuorumPeer.main(new String[]{this.command});
	    	} catch (Exception ex) {
	            trayIcon.displayMessage("HSearch Server", 
		                "Error during Server " + this.command + "\n" + ex.getMessage(),
                    TrayIcon.MessageType.ERROR);
	    	}
	    }
	}
	
	/**
	 * Starting hbase
	 * @author karan
	 *
	 */
	static class OHBase extends Thread {
	    String command = "start";
	    TrayIcon trayIcon = null;

	    public OHBase(String command, TrayIcon trayIcon) {
	    	this.command = command;
	    	this.trayIcon = trayIcon;
	    }
	    public void run() {
	    	try {
	    		HMaster.main(new String[]{command});
	    	} catch (Exception ex) {
	            trayIcon.displayMessage("Searchcherry Server", 
	                "Error during Server " + this.command + "\n" + ex.getMessage(),
	                TrayIcon.MessageType.ERROR);
	    	}
	    }
	}
}
