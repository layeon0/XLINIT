package xl.init.conn;


//gssg - xl m2m 최초 포팅 개발 - TODO


import java.sql.Connection;


import java.sql.PreparedStatement;
import java.sql.Statement;

import xl.lib.common.XLCons;
import xl.lib.common.XLDBManager;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.init.util.XLException;

public class XLCubridConnection{
	
	// Connection
	public Connection conn = null;
	
	//apply statement
	private PreparedStatement opstmt = null;
	private String dbIp = "";
	private String dbSid = "";
	private String userId = "";
	private String passwd = "";
	private int port = 1521;
	
	// private byte dbType = XLCons.ORACLE;
	private byte dbType = XLCons.CUBRID;

	
	public  XLCubridConnection(String _dbIp, String _dbSid, String _userId, String _passwd, int _port, byte _dbType){
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

	private void createConnection() {		
		
		XLDBManager dbmgr = new XLDBManager();
		Statement st = null;
		for ( int i = 0; i < XLConf.XL_DBCON_RETRYCNT; i++ ) {	
			try {
				

				conn = dbmgr.getConnection(
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
								

				st = conn.createStatement();		
				
				break;
			} catch(Exception e){

				e.printStackTrace();
				
				// gssg - Troubleshooting 에러 코드 생성
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
	/** connection을 종료한다. */
	public void closeConnection(){
		
		try { if(opstmt!=null)opstmt.close(); } catch(Exception e) {} finally { opstmt = null; }
		try { if(conn!=null)conn.close(); } catch(Exception e) {} finally { conn = null; }
		
	}
	
	public Connection getConnection()
	{
		return this.conn;
	}

	/** key로 Pstmt를 가지고 온다. */
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
