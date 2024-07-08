package xl.init.conn;


//gssg - xl m2m 최초 포팅 개발 - TODO


import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.Statement;

import xl.lib.common.XLCons;
import xl.lib.common.XLDBManager;
import xl.init.conf.XLConf;
import xl.init.dbmgr.XLDBCons;
import xl.init.logger.XLLogger;
import xl.init.util.XLException;

public class XLMySQLConnection{
	
	// Connection
	public Connection conn = null;
	// public Connection conn = null; // cksohn - for Altibase porting
	
	//apply statement
	private PreparedStatement opstmt = null;
	// private PreparedStatement opstmt = null; // cksohn - for Altibase porting

//	// cksohn - for otop LOB 복제 성능 개선 
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
	
	// private byte dbType = XLCons.ORACLE;
	// gssg - xl m2m 기능 추가
	private byte dbType = XLCons.MYSQL;

	
	public  XLMySQLConnection(String _dbIp, String _dbSid, String _userId, String _passwd, int _port, byte _dbType){
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


	/** Oracle Connection 생성 */
	// cksohn - for otop LOB 복제 성능 개선  - SRC DB 정보르 접속하도록 수정
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
				st.executeQuery("set global local_infile=TRUE");
				// gssg - p2p 하다가 m2m foreign key 무시 설정 추가
				st.executeQuery("SET foreign_key_checks = 0");
				try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
				
				// gssg - xl o2m 보완
				// gssg - MariaDB 커넥션 설정
				XLLogger.outputInfoLog("SET net_write_timeout=36000");
				st = conn.createStatement();		
				st.execute("SET net_write_timeout=36000");
				try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
										
				XLLogger.outputInfoLog("SET net_read_timeout=36000");
				st = conn.createStatement();
				st.execute("SET net_read_timeout=36000");
				try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
				
				// gssg - 전체적으로 보완_start_20221101
				// gssg - m2m TIMEZONE 설정 추가
				// gssg - LG엔솔 MS2O
				// gssg - o2o bulk ltz 보완
				if ( !XLConf.XL_TIMEZONE.equals("") && XLConf.XL_TIMEZONE != null ) {
					XLLogger.outputInfoLog("SET time_zone='" + XLConf.XL_TIMEZONE + "'");
					st = conn.createStatement();
					st.execute("SET time_zone='" + XLConf.XL_TIMEZONE + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}					
				}
				
				break;
			} catch(Exception e){
				// CKSOHN DEBUG
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
