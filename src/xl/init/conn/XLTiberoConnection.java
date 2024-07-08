package xl.init.conn;

 // cksohn - xl tibero src ��� �߰�
/**
 * 
 * cksohn - xl tibero src ��� �߰�
 * LOB Ÿ�� ������, APPLY ���� LOB data ������ ���� src db �� �����ϰ� �Ǵµ�. 
 * �̶�, APPLY ���� SRC DB ���ӹ� Prepared stmt ������ ���� ������ NRSRCConnection ��ü�� ����� �����ϵ��� �Ѵ�. 
 * 
 * ���� NRConnection ��ü�� �����ؼ� �����ϴµ�, ���ʿ��� �κ��� �����ϵ��� ����
 * 
 */


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import xl.lib.common.XLCons;
import xl.lib.common.XLDBManager;
import xl.init.conf.XLConf;
import xl.init.dbmgr.XLDBCons;
import xl.init.logger.XLLogger;
import xl.init.util.XLException;

public class XLTiberoConnection{
	
	// Connection
	public Connection conn = null;
	// public Connection conn = null; // cksohn - for Altibase porting
	
	//apply statement
	private PreparedStatement opstmt = null;
	// private PreparedStatement opstmt = null; // cksohn - for Altibase porting

//	// cksohn - for otop LOB ���� ���� ���� 
//	Hashtable ht_connInfo = null;
//	
//
//	public  XLMgrOracleConnection( Hashtable _ht_connInfo){
//		this.ht_connInfo = _ht_connInfo;
//	}
//	
	private String dbIp = "";
	private String dbSid = "";
	private String userId = "";
	private String passwd = "";
	private int port = 1521;
	private byte dbType = XLCons.TIBERO;

	
	public  XLTiberoConnection(String _dbIp, String _dbSid, String _userId, String _passwd, int _port, byte _dbType){
		this.dbIp = _dbIp;
		this.dbSid = _dbSid;
		this.userId = _userId;
		this.passwd = _passwd;
		this.port = _port;
		this.dbType = _dbType;
		
	}

	
	public boolean makeConnection(){
		createConnection();
		if(conn==null){

			XLLogger.outputInfoLog("DB From mgr Connection failed. Confirm  Database."); 
			return false;
		}
		// XLLogger.outputInfoLog("[INFO] DB Connected - " + this.dbIp + "/" + this.dbSid);
		return true;
	}



	// cksohn - for otop LOB ���� ���� ����  - SRC DB ������ �����ϵ��� ����
	private void createConnection() {		
		
		XLDBManager dbmgr = new XLDBManager();
		Statement st = null;
		for ( int i = 0; i < XLConf.XL_DBCON_RETRYCNT; i++ ) {	
			try {
				

				conn = (Connection)dbmgr.getConnection(
					this.dbType, 						
					this.dbIp,
					this.port,
					this.userId,
					this.passwd,
					this.dbSid);

				conn.setAutoCommit(false);	    	
				
				//XLLogger.outputInfoLog("########################################");
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DBUG] DEFAULT BATCH SIZE SET = " + XLConf.XL_BATCH_SIZE);
				}
				//XLLogger.outputInfoLog("########################################");
								
//				// Oracle ���� ����
//				if ( dbType == XLCons.ORACLE ) {					
//					conn.setDefaultExecuteBatch(XLConf.XL_BATCH_SIZE);
//					conn.setDefaultRowPrefetch(XLConf.XL_FETCH_SIZE);					
//				}
				
				
				// cksohn - src db connection �� sesssion date format start - [
				// date format ����
				st = conn.createStatement();		
				
				if ( dbType == XLCons.ORACLE || dbType == XLCons.TIBERO ) {
					st.executeQuery("alter session set nls_date_format='" + XLDBCons.DATE_FORMAT + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					
					//3.2.00-015 2011-07-11 modify for nls_timestamp format
					st = conn.createStatement();		
					st.executeQuery("alter session set nls_timestamp_format='" + XLDBCons.TIMESTAMP_FORMAT + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					
					//sykim 3.2.01-037 2013-03-06 for timestamp zone format
					st = conn.createStatement();		
					st.executeQuery("alter session set nls_timestamp_tz_format='" + XLDBCons.TIMESTAMP_TZ_FORMAT + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					
					// ] - end k cksohn - src db connection �� sesssion date format
				}
				
				
				break;
			} catch(Exception e){
				// CKSOHN DEBUG
				e.printStackTrace();
				
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1002] DB Connection by manager failed!("+ (i+1) +") " + this.dbIp + this.dbSid);
				try { if(conn != null) conn.close(); } catch(Exception e1) {} finally { conn = null; }
				try{
					Thread.sleep(10000);				
				}catch(Exception se){}
			} finally {
				try { if(st != null) st.close(); } catch(Exception e) {} finally { st = null; }
			}
			
		}

	}
	

// cksohn - for Altibase porting
	/** connection�� �����Ѵ�. */
	public void closeConnection(){
		
		try { if(opstmt!=null)opstmt.close(); } catch(Exception e) {} finally { opstmt = null; }
		try { if(conn!=null)conn.close(); } catch(Exception e) {} finally { conn = null; }
		
	}
	
	public Connection getConnection()
	{
		return this.conn;
	}

	/** key�� Pstmt�� ������ �´�. */
// cksohn - for Altibase porting
	// public PreparedStatement getPreparedStatement(String _key, String _sql){
	public PreparedStatement getPreparedStatement(String _key, String _sql){
		
		try {
			opstmt = (PreparedStatement)conn.prepareStatement(_sql);
		
			return opstmt;
			
		}catch (Exception e) {
				XLException.outputExceptionLog(e);
				return null;
		}
		
		
	}

	
	// return commit count
	public boolean commit()
	{
		try {
			this.conn.commit();
			
			return true;
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
			return false;
		}
	}
	

	public boolean rollback()
	{
		try {
			this.conn.rollback();
			
			return true;
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
			return false;
		}
	}

}
