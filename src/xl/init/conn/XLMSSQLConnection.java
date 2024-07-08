package xl.init.conn;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OraclePreparedStatement;
import xl.lib.common.XLCons;
import xl.lib.common.XLDBManager;
import xl.lib.common.XLQuery;
import xl.init.conf.XLConf;
import xl.init.dbmgr.XLDBCons;
import xl.init.logger.XLLogger;
import xl.init.util.XLException;

public class XLMSSQLConnection{
	
	// Connection
	public Connection conn = null;

	//apply statement
	private PreparedStatement opstmt = null;

	private String dbIp = "";
	private String dbSid = "";
	private String userId = "";
	private String passwd = "";
	private int port = 1433;
	private byte dbType = XLCons.MSSQL;


	// gssg - ms2ms 지원
	// gssg - IDENTITY 컬럼 처리
	private boolean isIdeintity = false;
	
	Vector identityVt = null;
	public  XLMSSQLConnection(String _dbIp, String _dbSid, String _userId, String _passwd, int _port, byte _dbType, boolean _isIdeintity) {
		this.dbIp = _dbIp;
		this.dbSid = _dbSid;
		this.userId = _userId;
		this.passwd = _passwd;
		this.port = _port;
		this.dbType = _dbType;
		this.isIdeintity = _isIdeintity;
		
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
					XLLogger.outputInfoLog("[DEBUG] DEFAULT BATCH SIZE SET = " + XLConf.XL_BATCH_SIZE);
				}
				//XLLogger.outputInfoLog("########################################");
				
				// gssg - ms2ms 지원
				// gssg - IDENTITY 컬럼 처리
				if ( isIdeintity ) {
					identityVt = getIdentityInfo(conn);					
				}
				
				st = conn.createStatement();
				
				
				
				
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
	
	// gssg - ms2ms 지원
	// gssg - IDENTITY 컬럼 처리
	public Vector getIdentityInfo(Connection _conn) {
		
		try {
			
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();

				query.append("SELECT B.NAME AS TB_TNAME") 
				.append(" FROM SYS.SYSCOLUMNS A JOIN SYS.SYSOBJECTS B ON B.ID = A.ID")
				.append(" WHERE A.STATUS = 128") 
				.append(" AND B.TYPE = 'U'") 
				.append(" AND B.NAME NOT LIKE 'QUEUE_MESSAGES_%'");

//			if ( XLConf.XL_MGR_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] getIdentityInfo sql = " + query.toString());
//			}

			Vector vt = xlQuery.getList(_conn, query.toString());						
			return vt;
			
		} catch (Exception e) {
			
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
			
		}
	}
	
	// gssg - ms2ms 지원
	// gssg - IDENTITY 컬럼 처리
	private XLQuery getXLQuery(){
		return  new XLQuery(
				this.dbIp,
				this.port,
				this.userId,
				this.passwd,
				this.dbSid
				);
	}

	// gssg - ms2ms 지원
	// gssg - IDENTITY 컬럼 처리
	public Vector getIdentityVt() {
		return identityVt;
	}

}
