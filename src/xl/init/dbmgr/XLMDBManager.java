package xl.init.dbmgr; 

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import com.edb.core.SetupQueryRunner;

import xl.lib.common.XLCons;
import xl.lib.common.XLDBManager;
import xl.lib.common.XLQuery;
import xl.init.conf.XLConf;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLMemInfo;
import xl.init.logger.XLLogger;
import xl.init.main.XLOGCons;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLMDBManager {
	
	
	// cksohn - scheduler 1.0 ���� ���� ����  - session date format 
	
//	public static final String DATE_FORMAT_ALTI = "YYYY-MM-DD HH24:MI:SS";	// ALTIBASE date type ��¥ ����
//	public static final String TIMESTAMP_FORMAT_ALTI = "YYYY-MM-DD HH24:MI:SS.SSSSSS";	// ALTIBASE TIMESTAMP ����
//	// cksohn - for sundb
//	public static final String DATE_FORMAT_SUNDB = "YYYY-MM-DD HH24:MI:SS";	
//	public static final String TIMESTAMP_FORMAT_SUNDB = "YYYY-MM-DD HH24:MI:SS.FF6";	
//	public static final String DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS";	// ����Ŭ date type ��¥ ����
//	//3.2.00-015 2011-07-11 modify for nls_timestamp format
//	public static final String TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SSXFF";	// ����Ŭ TIMESTAMP ����
//	// sykim 3.2.01-037 2013-03-06 for timestamp zone format
//	public static final String TIMESTAMP_TZ_FORMAT = "YYYY-MM-DD HH24:MI:SSxFF TZH:TZM";
	
//------------------------------------------------------------------------------------
// DB ���� �� ���� ���� ���� - START
//------------------------------------------------------------------------------------
	/**
	 * DB Connection
	 * @return
	 */
	public Connection createConnection(boolean isAutoCommit){	
		Connection con = null;
		XLDBManager dbmgr = new XLDBManager();	
		try {						
			
			
			for ( int i = 0; i < XLConf.XL_DBCON_RETRYCNT; i++ ) {				
				try {

					con = dbmgr.getConnection(XLConf.DBTYPE_STR,
							XLConf.DB_IP,
							XLConf.DB_PORT,
							XLConf.DB_ID,
							XLConf.DB_PASS,
							XLConf.DB_SID);	
	    			// ((OracleConnection) con).setAutoCommit(isAutoCommit); // cksohn - for Mysql Catalog DB
					con.setAutoCommit(isAutoCommit);
   			
					// cksohn - for otot
					if ( XLConf.DBTYPE_STR == XLCons.ORACLE ||
							XLConf.DBTYPE_STR == XLCons.TIBERO ) { // cksohn - for Mysql Catalog DB  - cksohn - catalog db connection for mysql - connection �κм��� & sysdate & rownum
		    			// date format ����
		    			Statement st = con.createStatement();		
		    			st.executeQuery("alter session set nls_date_format='" + XLDBCons.DATE_FORMAT + "'");
		    			try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
	
						//3.2.00-015 2011-07-11 modify for nls_timestamp format
						st = con.createStatement();		
						st.executeQuery("alter session set nls_timestamp_format='" + XLDBCons.TIMESTAMP_FORMAT + "'");
						try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
						// cksohn- tibero4 nls_timestamp_tz_format �̽� - comment
						//sykim 3.2.01-037 2013-03-06 for timestamp zone format
//						st = con.createStatement();		
//						st.executeQuery("alter session set nls_timestamp_tz_format='" + NRAPManager.TIMESTAMP_TZ_FORMAT + "'");
//						try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					}		
										
	    			break;	    			
				} catch(Exception e){
					// gssg - Troubleshooting ���� �ڵ� ����
					// gssg - ���������ڿ������� �������̰� ���
					XLLogger.outputInfoLog("[E1001] XL Catalog Connection failed!! DB reconnection attempt("+ (i+1) +")");
					try { if(con != null) con.close(); } catch(Exception e1) {} finally { con = null; }
					Thread.sleep(10000);
				}
				
//				NRAPManager.outputInfoLog("CKSOHN DEBUG>>>>>>>>> isApplyError = " + NRAPManager.isApplyError);
				
//				//sykim 3.2.01-023 2012-09-19 for db connect retry
//				//DB connection ������ ���� ��� / ���� ���� ��û �� connection retry break
				//if(NRAPManager.isApplyError)break; // cksohn - scheduler 1.0 ���� ���� ���� - comment
				
			} // for-end			
		} catch (Exception e) {
			// cksohn - connection �κ� simple Exception �޽��� ���
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());

		}

		return con;
	}
	/**
	 * DB Connection close
	 * @return
	 */
	public void closeConnection(Connection con){	
		try{if(con!=null)con.close();}catch(Exception e){}finally{con=null;}
	}
	/**
	 * DB Connection commit
	 * @return
	 */
	public void commitConnection(Connection con){	
		try {
			if(con!=null)con.commit();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * DB Connection rollback
	 * @return
	 */
	public void rollbackConnection(Connection con){	
		try {
			if(con!=null)con.rollback();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 * executeQuery - Select ����� �Ѱ��� 
	 * XLQuery���� Long Type�� ó���� �����ִ� �� �ϴ�.
	 * �׷��� ������ ����
	 * @param _ipaddr
	 * @param _port
	 * @param _id
	 * @param _pass
	 * @param _sid
	 * @param sb_sql
	 * @param vt_col
	 * @return Vector<Hashtable<String,String>>
	 */
	private Vector<Hashtable<String,String>> getResultForSql(
			byte _dbType, String _ipaddr, int _port, String _id, String _pass, 
			String _sid, StringBuffer sb_sql, Vector<String> vt_col){		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Vector<Hashtable<String,String>> vt_res = null;
		try{
			conn = createConnection(true);	
			
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sb_sql.toString());			
			vt_res = new Vector<Hashtable<String,String>>();
			while(rs.next()){
				Hashtable<String,String> ht_res = new Hashtable<String, String>();
				for(int i=0;i<vt_col.size();i++){
					String key = vt_col.get(i);
					String value = rs.getString(key);
					if(value==null)value="";
					ht_res.put(key, value);
				}
				vt_res.add(ht_res);
			}			
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}finally{
			try{if(rs!=null)rs.close();}catch(Exception e){}finally{rs=null;}
			try{if(stmt!=null)stmt.close();}catch(Exception e){}finally{stmt=null;}
			try{if(conn!=null)conn.close();}catch(Exception e){}finally{conn=null;}
		}
		return vt_res;
	}
	
	private Vector<Hashtable<String,String>> getResultForSql(
			Connection conn, StringBuffer sb_sql, Vector<String> vt_col){		
		Statement stmt = null;
		ResultSet rs = null;
		Vector<Hashtable<String,String>> vt_res = null;
		try{
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sb_sql.toString());			
			vt_res = new Vector<Hashtable<String,String>>();
			while(rs.next()){
				Hashtable<String,String> ht_res = new Hashtable<String, String>();
				for(int i=0;i<vt_col.size();i++){
					String key = vt_col.get(i);
					String value = rs.getString(key);
					if(value==null)value="";
					ht_res.put(key, value);
				}
				vt_res.add(ht_res);
			}			
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}finally{
			try{if(rs!=null)rs.close();}catch(Exception e){}finally{rs=null;}
			try{if(stmt!=null)stmt.close();}catch(Exception e){}finally{stmt=null;}
		}
		return vt_res;
	}
	
	// cksohn 3.3.00-001 2014-12-25 for DDL Create Table & DDL - �������� 
	/**
	 * executeQuery - Select ����� �Ѱ��� 
	 * XLQuery���� Long Type�� ó���� �����ִ� �� �ϴ�.
	 * �׷��� ������ ����
	 * @param sb_sql
	 * @param vt_col
	 * @return Vector<Hashtable<String,String>>
	 */	
	private Vector<Hashtable<String,String>> getResultForSql(StringBuffer sb_sql, Vector<String> vt_col){		
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		Vector<Hashtable<String,String>> vt_res = null;
		try{
			conn = createConnection(true);	
			
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sb_sql.toString());			
			
			//NRAPManager.outputInfoLog("CKSOHN DEBUG------ sb_sql.toString() = " + sb_sql.toString());
			
			vt_res = new Vector<Hashtable<String,String>>();
			while(rs.next()){
				//NRAPManager.outputInfoLog("CKSOHN DEBUG------ rs.next() is exist");
				Hashtable<String,String> ht_res = new Hashtable<String, String>();
				for(int i=0;i<vt_col.size();i++){
					String key = vt_col.get(i);
					//NRAPManager.outputInfoLog("CKSOHN DEBUG------------- key = " + key);
					String value = rs.getString(key);
					if(value==null)value="";
					ht_res.put(key, value);
				}
				vt_res.add(ht_res);
			}			
		}catch(Exception e){
			e.printStackTrace();
			return null;
		}finally{
			try{if(rs!=null)rs.close();}catch(Exception e){}finally{rs=null;}
			try{if(stmt!=null)stmt.close();}catch(Exception e){}finally{stmt=null;}
			try{if(conn!=null)conn.close();}catch(Exception e){}finally{conn=null;}
		}
		return vt_res;
	}
	

	
	// ayzn - XLInit ��� ����  - DBManager : CDCīŻ�α� conf �������� ����
	private XLQuery getXLQuery(){
		return  new XLQuery(
				XLConf.DB_IP,
				XLConf.DB_PORT,
				XLConf.DB_ID,
				XLConf.DB_PASS,
				XLConf.DB_SID);
	}
	
	private boolean executeUpdateStatement(Connection con, String sql){
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.executeUpdate(sql);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}finally{
			try {stmt.close();}catch (Exception e){}finally{stmt=null;}
		}
		return true;
	}
//------------------------------------------------------------------------------------
// DB ���� �� ���� ���� ���� - END
//------------------------------------------------------------------------------------
	
	
//------------------------------------------------------------------------------------
// NR_SVR , NR_DBMS , NR_GRP , NR_POL - START
//------------------------------------------------------------------------------------	


	// Sched port ���� ����
	// ������ ����� : return -1
	public int getSchedPort() {
		
		try {
			XLQuery nrQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			query.append("SELECT CONF_VALUE FROM XL_CONF WHERE CONF_OPTION='XL_SCHED_PORT'");
		
			Vector vt = nrQuery.getList(XLConf.DBTYPE_STR, query.toString());
			if ( vt == null || vt.size() == 0 ) {
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1003] Faile to get Scheduler port information.");
				return -1;
			} else {
				Hashtable ht = (Hashtable)vt.get(0);			
				int port = Integer.parseInt((String)ht.get("CONF_VALUE"));
				return port;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}
	
	// cksohn - scheduler 1.0 ���� ���� ����
	// XLConf ���� conf �� read
	public Vector getConfValues() {
		
		try {
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//			query.append("SELECT CONF_OPTION, CONF_VALUE FROM XL_CONF");
			query.append("SELECT T1.CONF_OPTION, T1.CONF_VALUE") 
			.append(" FROM XL_CONF T1, XL_WORKPLAN T2") 
			.append(" WHERE T1.CONF_WORKPLAN_SEQ = T2.WORKPLAN_SEQ")
			.append(" AND T2.WORKPLAN_NAME = '" + XLConf.XL_WORKPLAN_NAME + "'");

			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
			// if ( vt == null || vt.size() == 0 ) {
			if ( vt == null || vt.size() == 0 ) {
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1004] Failed to get conf information.");
				return null;
			}
			// XLLogger.outputInfoLog("CKSOHN DEBUG Conf values size = " + vt.size());
			
			return vt;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// cksohn - scheduler 1.0 ���� ���� ����
	// catalog db�� ���� Ȱ��ȭ ������ ���� ����
	public Vector getActiveSchedule() {
		
		try {
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			query.append("SELECT DISTINCT A.SCHED_NAME, A.SCHED_QUARTZ") 
				.append(" FROM XL_SCHED A, XL_POL B") 
				.append(" WHERE A.SCHED_SEQ = B.POL_SCHED_SEQ") 
				.append(" AND B.POL_SCHED_ACT_YN='Y'");
			
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
			
//			if ( vt == null || vt.size() == 0 ) {
//				XLLogger.outputInfoLog("[EXCEPTION] Failed to get conf information.");
//				return null;
//			}
			if ( vt == null ) {
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1005] Failed to get active schedule information.");
			} else {
				XLLogger.outputInfoLog("The number of active schedule = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// ������ JOB ���� ��� ����
	public Vector getJobList(String _schedName) {
		
		try {
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			query.append("SELECT") 
					.append(" B.POL_NAME,") 
					.append(" B.POL_RETRY_YN,")
					.append(" B.POL_PRIORITY,") 
					.append(" B.POL_DAY_JOBCNT,") 
					.append(" C.CONDITION_SEQ,") 
					.append(" C.CONDITION_WHERE,") 
					.append(" C.CONDITION_ACTION") 
					.append(" FROM XL_SCHED A, XL_POL B, XL_CONDITION C") 
					.append(" WHERE A.SCHED_NAME='" + _schedName + "'") 
					.append(" AND A.SCHED_SEQ=B.POL_SCHED_SEQ") 
					.append(" AND B.POL_NAME=C.CONDITION_POLNAME") 
					.append(" AND B.POL_SCHED_ACT_YN='Y'") 
					.append(" AND C.CONDITION_ACTION<>'S'") // �̼���, ����, ����, ��� �۾� ��� ����
					// gssg - ��� ����
					// gssg - �켱���� ����
//					.append(" ORDER BY B.POL_PRIORITY DESC, B.POL_NAME ASC, C.CONDITION_SEQ ASC");
					.append(" ORDER BY B.POL_PRIORITY ASC, B.POL_NAME ASC, C.CONDITION_SEQ ASC");
			
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
			
//			if ( vt == null || vt.size() == 0 ) {
//				XLLogger.outputInfoLog("[EXCEPTION] Failed to get conf information.");
//				return null;
//			}
			if ( vt == null ) {
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1006] Failed to get job list information. (schedule : " + _schedName);
			} else {
				// XLLogger.outputInfoLog("The number of active schedule = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// cksohn - scheduler 1.0 ���� ���� ���� JOBQ ��� PreparedStament ����
	public PreparedStatement getPstmtInsertJobQ(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		query.append("INSERT INTO XL_JOBQ(JOBQ_SEQ, JOBQ_POLNAME, JOBQ_CONDITION_WHERE, JOBQ_STATUS, JOBQ_PRIORITY, JOBQ_CDATE)") 
				.append(" VALUES (SEQ_XL_JOBQ.NEXTVAL, ?, ?, 'W', ?, SYSDATE)");
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
		}catch(Exception e){
			// cksohn - connection �κ� simple Exception �޽��� ���
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());

		}		
		return pstmt;
	}

	
	// update JobQStatus PreparedStatement
	public PreparedStatement getPstmtUpdateJobQStatus(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		query.append("UPDATE XL_JOBQ SET JOBQ_STATUS=?, JOBQ_SDATE=? WHERE JOBQ_SEQ=? AND JOBQ_POLNAME=?");
				
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
			
		}catch(Exception e){
			// cksohn - connection �κ� simple Exception �޽��� ���
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());
			return null;

		}		
		return pstmt;
	}
	
	
	// update JobQ commit cnt PreparedStatement
	public PreparedStatement getPstmtUpdateJobQCommitCnt(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		query.append("UPDATE XL_JOBQ SET JOBQ_COMMIT_CNT=? WHERE JOBQ_SEQ=? AND JOBQ_POLNAME=?");
				
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
			
		}catch(Exception e){
			// cksohn - connection �κ� simple Exception �޽��� ���
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());
			return null;

		}		
		return pstmt;
	}
	
	// update CONDITION commit cnt PreparedStatement
	public PreparedStatement getPstmtUpdateCondCommitCnt(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//		query.append("UPDATE XL_CONDITION SET CONDITION_COMMIT_CNT=? WHERE CONDITION_POLNAME=? AND CONDITION_SEQ=?");
		query.append("UPDATE XL_CONDITION SET CONDITION_COMMIT_CNT=? WHERE CONDITION_POLNAME=? AND CONDITION_SEQ=? AND CONDITION_WORKPLAN_SEQ=?");
				
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
			
		}catch(Exception e){
			// cksohn - connection �κ� simple Exception �޽��� ���
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());
			return null;

		}		
		return pstmt;
	}
	
	// MGR
	// XL_DBMS ���̺� ���� ����
	// public Vector getDbmsInfo() {
	public Vector getDbmsInfo() {
		
		try {
		
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			
			// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//			query.append("SELECT * FROM XL_DBMS");
			
			// ayzn - XLInit ��� ����  - DBManager : getDbmsInfo�Լ� ���� CDC īŸ�α� ���̺� ������ ����
			/*query.append("SELECT * FROM XL_DBMS T1, XL_WORKPLAN T2")
			.append(" WHERE T2.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
			.append(" AND T1.DBMS_WORKPLAN_SEQ=T2.WORKPLAN_SEQ");*/
			
			query.append("SELECT * FROM NR_SVR T1, NR_DBMS T2, XL_CHARENCODE T3")
			.append(" WHERE T1.SVR_SEQ=T2.SVR_SEQ")
			.append(" AND T2.DBMS_SEQ=T3.ENCODE_DBSEQ");
			
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());	
						
			if ( vt == null ) {
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1007] Failed to get dbms information.");
			} else {
				XLLogger.outputInfoLog("The number of dbms info = " + vt.size());
			}
						
			
			return vt;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	// -- 1. JobQ�� ��ϵ� �۾��� ���� �ҽ� DBMS���� Running ���� JOB�� count ����
	// SELECT  JOBQ_SDBIP, JOBQ_SDBSID, COUNT(*) AS CNT 
	// FROM XL_JOBQ
	// WHERE JOBQ_STATUS='R' 
	// GROUP BY JOBQ_SDBIP, JOBQ_SDBSID;
	public Vector getRJobCntByDbms(Connection _conn) {
		
		try {
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//				query.append("SELECT JOBQ_SDBIP, JOBQ_SDBSID, COUNT(CASE WHEN JOBQ_STATUS='R' THEN 1 END ) AS RCNT")
//						.append(" FROM XL_JOBQ")
			query.append("SELECT T1.JOBQ_SDBIP, T1.JOBQ_SDBSID, COUNT(CASE WHEN T1.JOBQ_STATUS='R' THEN 1 END ) AS RCNT")
					.append(" FROM XL_JOBQ T1, XL_WORKPLAN T2")
					.append(" WHERE T2.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
					.append(" AND T1.JOBQ_WORKPLAN_SEQ=T2.WORKPLAN_SEQ")
//						.append(" GROUP BY JOBQ_SDBIP, JOBQ_SDBSID");
					.append(" GROUP BY T1.JOBQ_SDBIP, T1.JOBQ_SDBSID");
			
			// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
			Vector vt = xlQuery.getList(_conn, query.toString());
			

			if ( vt == null ) {
				XLLogger.outputInfoLog("[EXCEPTION] Failed to get JobQ information.");
			} else {
				// XLLogger.outputInfoLog("The number of Running job = " + vt.size());
				for ( int i=0; i<vt.size(); i++ ) {
					Hashtable ht = (Hashtable)vt.get(i);
					XLLogger.outputInfoLog("The number of Running job = " + (String)ht.get("JOBQ_SDBIP") + "/" +  (String)ht.get("RCNT"));
				}
			}
			
			return vt;
			
		} catch (Exception e) {
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
		}
	}
	
	// ���డ���� JobQ�� ��å���� ���� Running ���� ��å�� jobCnt 
	/**
	 * SELECT T1.JOBQ_POLNAME, T2.POL_TMAX_JOBCNT, COUNT(CASE WHEN T1.JOBQ_STATUS='R' THEN 1 END ) AS RCNT
		FROM XL_JOBQ T1, XL_POL T2
		WHERE ( (T1.JOBQ_SDBIP='192.168.0.28' AND T1.JOBQ_SDBSID='orcl100')
		OR (T1.JOBQ_SDBIP='192.168.0.xx' AND T1.JOBQ_SDBSID='orcl100')
		OR ( T1.JOBQ_SDBIP='192.168.0.yy' AND T1.JOBQ_SDBSID='XXX')
		OR ( T1.JOBQ_SDBIP='192.168.0.zzz' AND T1.JOBQ_SDBSID='orcl100')
		OR (T1.JOBQ_SDBIP='192.168.0.28' AND T1.JOBQ_SDBSID='orcl15'))
		AND T1.JOBQ_POLNAME=T2.POL_NAME
		GROUP BY T1.JOBQ_POLNAME, T2.POL_TMAX_JOBCNT;
	 */
	public Vector getRJobCntByPol(Connection _conn) {
		
		try {
			
			//XLLogger.outputInfoLog("CKSOHN DEBUG HT_JOBQ_DBMS_TMP = " + XLMemInfo.HT_JOBQ_DBMS_TMP);
			
			if ( XLMemInfo.HT_JOBQ_DBMS_TMP.size() == 0 ) {
				// ��ϵȰ� ����
				return new Vector();
			}
			
			
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();

			// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
			query.append("SELECT T1.JOBQ_POLNAME, T2.POL_TMAX_JOBCNT, COUNT(CASE WHEN T1.JOBQ_STATUS='R' THEN 1 END ) AS RCNT")
//					.append(" FROM XL_JOBQ T1, XL_POL T2")
					.append(" FROM XL_JOBQ T1, XL_POL T2, XL_WORKPLAN T3")
					.append(" WHERE (");
			
			int idx = 0;
			Enumeration<String> enuKeys = XLMemInfo.HT_JOBQ_DBMS_TMP.keys();
			
			while(enuKeys.hasMoreElements()){
				idx++;
				if ( idx != 1 ) {
					query.append(" OR");
				}
				String sdbIp = enuKeys.nextElement(); 
				String sdbSid = XLMemInfo.HT_JOBQ_DBMS_TMP.get(sdbIp);
				
				query.append(" (T1.JOBQ_SDBIP='").append(sdbIp).append("' AND T1.JOBQ_SDBSID='").append(sdbSid).append("')");
			} // while-end
					
			query.append(" )"); // WHERE OR ���ǵ鿡 ���� brace end
			query.append(" AND T1.JOBQ_POLNAME=T2.POL_NAME");
			// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
			query.append(" AND T3.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
			.append(" AND T1.JOBQ_WORKPLAN_SEQ=T3.WORKPLAN_SEQ")
			.append(" AND T2.POL_WORKPLAN_SEQ=T3.WORKPLAN_SEQ");			
			
			query.append(" GROUP BY T1.JOBQ_POLNAME, T2.POL_TMAX_JOBCNT");
			
			//if ( XLConf.XL_MGR_DEBUG_YN ) {
			//	XLLogger.outputInfoLog("[DEBUG] getRJobCntByPol Query = " + query.toString());
			//}
			
			// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
			Vector vt = xlQuery.getList(_conn, query.toString());
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[EXCEPTION] Failed to get Running Job by policy information.");
			} else {
				XLLogger.outputInfoLog("The number of Running job by policy = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) {
			
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
			
		}
	}
		
	/**
	 * SELECT JOBQ_SEQ, JOBQ_POLNAME, JOBQ_CONDITION_WHERE
		FROM XL_JOBQ
		WHERE JOBQ_STATUS='W'
		AND JOBQ_POLNAME IN ('P_111')
		ORDER BY JOBQ_PRIORITY DESC, JOBQ_SEQ ASC, JOBQ_POLNAME ASC;
	 */
	// ��� ���͸��� ��ģ JOBQ�� ��å�鿡 ���� ���� ������ ����
	// �� ������ ��Ȳ�� ���� ���� ���̰� 4000 byte �̻��� �ɼ��� �����Ƿ�, 
	// XLQuery�� ������� �ʰ�, PreparedStatement �������� ���� XLQuery�� ������ ���·� return �� �ֵ��� �Ѵ�.
	public Vector getJobToRun(Connection _conn) {
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Vector vt = new Vector();
		
		try {		
						
			
			if ( XLMemInfo.HT_JOBQ_POL_CNT_TMP.size() == 0 ) {
				return new Vector();
			}
			
			StringBuffer query = new StringBuffer();

			// query.append("SELECT T1.JOBQ_SEQ, T1.JOBQ_POLNAME, T1.JOBQ_CONDITION_WHERE, T2.POL_TMAX_JOBCNT")
			// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
			// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
			query.append("SELECT T1.JOBQ_SEQ, T1.JOBQ_POLNAME, T1.JOBQ_CONDITION_WHERE, T2.POL_TMAX_JOBCNT, T3.DBMS_IP, T3.DBMS_SID")
					// .append(" FROM XL_JOBQ T1, XL_POL T2")
//						.append(" FROM XL_JOBQ T1, XL_POL T2, XL_DBMS T3") // cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					.append(" FROM XL_JOBQ T1, XL_POL T2, XL_DBMS T3, XL_WORKPLAN T4") // cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					.append(" WHERE T1.JOBQ_STATUS='W'")
					.append(" AND T2.POL_SDB_SEQ=T3.DBMS_SEQ") // cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					.append(" AND T1.JOBQ_POLNAME IN (");
			
			int polCnt = XLMemInfo.HT_JOBQ_POL_CNT_TMP.size();
			
			for (int i=0; i<polCnt; i++ ) {
				if ( i != 0 ) {
					query.append(",?");
				} else {
					query.append("?");
				}
			}
			query.append(")");
			query.append(" AND T1.JOBQ_POLNAME=T2.POL_NAME");
			query.append(" AND T1.JOBQ_WORKPLAN_SEQ=T4.WORKPLAN_SEQ");
			query.append(" AND T4.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'");
			// gssg - ��� ����
			// gssg - �켱���� ����
//				query.append(" ORDER BY T1.JOBQ_PRIORITY DESC, T1.JOBQ_SEQ ASC, T1.JOBQ_POLNAME ASC");
			query.append(" ORDER BY T1.JOBQ_PRIORITY ASC, T1.JOBQ_POLNAME ASC, T1.JOBQ_SEQ ASC");
			
			// PreparedStatement ����
			pstmt = _conn.prepareStatement(query.toString());
			
			
			int idx = 1;
			Enumeration<String> enuKeys = XLMemInfo.HT_JOBQ_POL_CNT_TMP.keys();
			while(enuKeys.hasMoreElements()){
				
				String polName = enuKeys.nextElement(); 
				
				pstmt.setString(idx, polName);				
				idx++;
			} // while-end
					

			// Query ����
			rs = pstmt.executeQuery();
			
			while ( rs.next() ) {
				
				String jobSeq = Long.toString(rs.getLong("JOBQ_SEQ"));
				String polName = rs.getString("JOBQ_POLNAME");
				String condWhere = rs.getString("JOBQ_CONDITION_WHERE");
				String tMaxCnt = Integer.toString(rs.getInt("POL_TMAX_JOBCNT"));

				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				String sdbIp = rs.getString("DBMS_IP");
				String sdbSid = rs.getString("DBMS_SID");

				
				Hashtable ht = new Hashtable();
				ht.put("JOBQ_SEQ", jobSeq);
				ht.put("JOBQ_POLNAME", polName);
				ht.put("JOBQ_CONDITION_WHERE", condWhere);
				ht.put("POL_TMAX_JOBCNT", tMaxCnt);
				
				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				ht.put("DBMS_IP", sdbIp);
				ht.put("DBMS_SID", sdbSid);
				
				
				vt.add(ht);				
				
			}
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[EXCEPTION] Failed to get JobQ information to Run.");
			} else {
				XLLogger.outputInfoLog("The number of JobQ information to Run = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) {
			
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
			
		} finally {
			
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }
			try { if ( pstmt != null ) pstmt.close(); } catch (Exception e) {} finally { pstmt = null; } 
		}
	}
	
	// ayzn - XLInit ��� ����  - DBManager : getPolInfo �Լ� �߰� ( Source, Target(�ϴ�� ��å�� ��  NR_POL��  DBMS_SEQ ����) ���� ���� )
	public Vector getPolInfo(String grpCode, String pol_name) {
		
		try {
		
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			
			query.append("SELECT T1.GRP_TYPE, T1.DBMS_SEQ, T1.SVR_SEQ, (SELECT SVR_IPADDR FROM NR_SVR WHERE SVR_SEQ = T1.SVR_SEQ) AS SVR_IPADDR, (SELECT DBMS_SID FROM NR_DBMS WHERE SVR_SEQ = T1.SVR_SEQ AND DBMS_SEQ = T1.DBMS_SEQ) AS DBMS_SID")
			.append(" FROM NR_GROUP T1, NR_POL T2 ")
			.append(" WHERE T2.POL_CODE = '"+pol_name+"' AND T2.GRP_CODE = '"+grpCode+"' AND T2.GRP_CODE = T1.GRP_CODE AND T1.GRP_TYPE = 'S' ")
			.append(" UNION ALL")
			.append(" SELECT T1.GRP_TYPE, T1.DBMS_SEQ, T1.SVR_SEQ, (SELECT SVR_IPADDR FROM NR_SVR WHERE SVR_SEQ = T1.SVR_SEQ) AS SVR_IPADDR, (SELECT DBMS_SID FROM NR_DBMS WHERE SVR_SEQ = T1.SVR_SEQ AND DBMS_SEQ = T1.DBMS_SEQ) AS DBMS_SID")
			.append(" FROM NR_GROUP T1, NR_POL T2 ")
			.append(" WHERE T2.POL_CODE = '"+pol_name+"' AND T2.GRP_CODE = '"+grpCode+"' AND T2.GRP_CODE = T1.GRP_CODE AND T1.GRP_TYPE = 'T' AND T1.DBMS_SEQ IN (SELECT DBMS_SEQ FROM NR_POL WHERE POL_CODE = '"+pol_name+"') ");
					
			
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());	
			
			XLLogger.outputInfoLog("[Source, Target INFO] getPolInfo sql : " + query.toString());
			
			if ( vt == null ) {
				// gssg - Troubleshooting ���� �ڵ� ����
				XLLogger.outputInfoLog("[E1007] Failed to get dbms information.");
			} else {
				XLLogger.outputInfoLog("The number of dbms info = " + vt.size());
			}
						
			
			return vt;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// �� ������ ��Ȳ�� ���� ���� ���̰� 4000 byte �̻��� �ɼ��� �����Ƿ�, 
	// XLQuery�� ������� �ʰ�, PreparedStatement �������� ���� XLQuery�� ������ ���·� return �� �ֵ��� �Ѵ�.
	// ayzn - XLInit ��� ����  - DBManager : getSourceInfo �Լ� �߰� ( SOURCE ���� ���� )
	public Vector getSourceInfo(Connection _conn, String grp_code, String pol_code, String dicOwner, String dicTname) {
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		Vector vt = new Vector();
		
		try {		
			StringBuffer query = new StringBuffer();
		
			query.append("SELECT POL_NAME, SVR_IPADDR, DBMS_SID, TB_CONDITION ")
			.append(" FROM NR_SVR T1, NR_DBMS T2, NR_GROUP T3, NR_POL T4, NR_CLONETB T5")
			.append(" WHERE T5.TB_OWNER = '"+dicOwner+"'")
			.append(" AND T5.TB_NAME = '"+dicTname+"'")
			.append(" AND T5.POL_CODE = '"+pol_code+"' AND T5.GRP_CODE = '"+grp_code+"'")
			.append(" AND T4.POL_CODE = '"+pol_code+"' AND T4.GRP_CODE = '"+grp_code+"'")
			.append(" AND T5.GRP_CODE = T4.GRP_CODE")
			.append(" AND T5.POL_CODE = T4.POL_CODE")
	        .append(" AND T4.GRP_CODE = T3.GRP_CODE")
			.append(" AND T4.GRP_TYPE = T3.GRP_TYPE")
			.append(" AND T3.DBMS_SEQ = T2.DBMS_SEQ") 
			.append(" AND T3.SVR_SEQ = T1.SVR_SEQ") 
			.append(" AND T2.SVR_SEQ = T1.SVR_SEQ");		
	
			// PreparedStatement ����
			pstmt = _conn.prepareStatement(query.toString());
			
			XLLogger.outputInfoLog("[Source INFO] [getInfo sql::]"+query.toString());
		
			// Query ����
			rs = pstmt.executeQuery();
			
			while ( rs.next() ) {
				
				String polName = rs.getString("POL_NAME");
				String sdbIp = rs.getString("SVR_IPADDR");
				String sdbSid = rs.getString("DBMS_SID");
				String tbCondition = "NONE";
				
				if(rs.getString("TB_CONDITION")!=null && !rs.getString("TB_CONDITION").equals(""))
				{
					tbCondition = rs.getString("TB_CONDITION");
				}
				
				Hashtable ht = new Hashtable();
				ht.put("POL_NAME", polName);
				ht.put("DBMS_IP", sdbIp);
				ht.put("DBMS_SID", sdbSid);
				ht.put("TB_CONDITION", tbCondition);
				
				vt.add(ht);				
				
			}
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[EXCEPTION] Failed to get source db information to Run.");
			} else {
				XLLogger.outputInfoLog("The number of source db information to Run = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) { 
			
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
			
		} finally {
			
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }
			try { if ( pstmt != null ) pstmt.close(); } catch (Exception e) {} finally { pstmt = null; } 
		}
	}
	
	// ayzn - XLInit ��� ����  - DBManager : getJobRunPolInfo �Լ�  ���� ( CDCīŻ�α� �����Ͽ� �÷����� ���� )
	public Vector getJobRunPolInfo(Connection _conn, String grpCode, String pol_name, String dicOwner, String dicTname) {
		
		try {
			
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();

			StringBuffer lobQuery = new StringBuffer();			
			if(	XLConf.DBTYPE_SRC == XLCons.ORACLE || XLConf.DBTYPE_SRC == XLCons.TIBERO || XLConf.DBTYPE_SRC == XLCons.ALTIBASE || XLConf.DBTYPE_SRC == XLCons.ALTIBASE5 || XLConf.DBTYPE_SRC == XLCons.CUBRID ) {
				
				  query.append("SELECT *")
				  .append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				  .append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				  .append(" AND T4.POL_CODE='"+pol_name+"'") 
				  .append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				  .append(" AND T4.TB_NAME='"+dicTname+"'") 
				  .append(" AND T1.GRP_TYPE='S'")
				  .append(" AND T3.DIC_CLONE_YN='Y'")
				  .append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				  .append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				  .append(" AND T4.POL_CODE=T3.POL_CODE")
				  .append(" AND T4.GRP_CODE=T3.GRP_CODE")
				  .append(" AND T3.POL_CODE=T2.POL_CODE") 
				  .append(" AND T2.GRP_CODE=T1.GRP_CODE") 
				  .append(" AND T3.DIC_DATATYPE NOT IN ('CLOB', 'NCLOB', 'BLOB', 'LONG', 'LONG RAW', 'XMLTYPE')")
				  .append(" ORDER BY T3.DIC_COLID ASC");
				  
				  lobQuery.append("SELECT *")
				  .append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				  .append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				  .append(" AND T4.POL_CODE='"+pol_name+"'") 
				  .append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				  .append(" AND T4.TB_NAME='"+dicTname+"'") 
				  .append(" AND T1.GRP_TYPE='S'")
				  .append(" AND T3.DIC_CLONE_YN='Y'")
				  .append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				  .append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				  .append(" AND T4.POL_CODE=T3.POL_CODE")
				  .append(" AND T4.GRP_CODE=T3.GRP_CODE")
				  .append(" AND T3.POL_CODE=T2.POL_CODE") 
				  .append(" AND T2.GRP_CODE=T1.GRP_CODE") 
				  .append(" AND T3.DIC_DATATYPE IN ('CLOB', 'NCLOB', 'BLOB', 'LONG', 'LONG RAW', 'XMLTYPE')")
				  .append(" ORDER BY T3.DIC_COLID ASC");
				 
												
			} 
			// gssg - m2m LOB �÷� ���� ������ ó��
			else if(	XLConf.DBTYPE_SRC == XLCons.MYSQL || XLConf.DBTYPE_SRC == XLCons.MARIADB ) {
				query.append("SELECT *") 
				.append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				.append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				.append(" AND T4.POL_CODE='"+pol_name+"'") 
				.append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				.append(" AND T4.TB_NAME='"+dicTname+"'") 
				.append(" AND T1.GRP_TYPE='S'")
				.append(" AND T3.DIC_CLONE_YN='Y'")
				.append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				.append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				.append(" AND T4.POL_CODE=T3.POL_CODE")
				.append(" AND T4.GRP_CODE=T3.GRP_CODE")
				.append(" AND T3.POL_CODE=T2.POL_CODE") 
				.append(" AND T2.GRP_CODE=T1.GRP_CODE") 
				.append(" AND T3.DIC_DATATYPE NOT IN ('TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB')")
				.append(" ORDER BY T3.DIC_COLID ASC");
	
				lobQuery.append("SELECT *") 
				.append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				.append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				.append(" AND T4.POL_CODE='"+pol_name+"'") 
				.append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				.append(" AND T4.TB_NAME='"+dicTname+"'") 
				.append(" AND T1.GRP_TYPE='S'")
				.append(" AND T3.DIC_CLONE_YN='Y'")
				.append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				.append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				.append(" AND T4.POL_CODE=T3.POL_CODE")
				.append(" AND T4.GRP_CODE=T3.GRP_CODE")
				.append(" AND T3.POL_CODE=T2.POL_CODE") 
				.append(" AND T2.GRP_CODE=T1.GRP_CODE") 
				.append(" AND T3.DIC_DATATYPE IN ('TINYTEXT', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB')")
				.append(" ORDER BY T3.DIC_COLID ASC");
				
			} else if ( XLConf.DBTYPE_SRC == XLCons.PPAS || XLConf.DBTYPE_SRC == XLCons.POSTGRESQL ) {
				// gssg - xl p2p ����
				// gssg - p2p LOB �÷� ���� ������ ó��
				// gssg - xl ��ü������ ����2
				// gssg - PostgreSQL Ŀ���� ó��					

				
				  query.append("SELECT *")
				  .append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				  .append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				  .append(" AND T4.POL_CODE='"+pol_name+"'") 
				  .append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				  .append(" AND T4.TB_NAME='"+dicTname+"'") 
				  .append(" AND T1.GRP_TYPE='S'")
				  .append(" AND T3.DIC_CLONE_YN='Y'")
				  .append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				  .append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				  .append(" AND T4.POL_CODE=T3.POL_CODE")
				  .append(" AND T4.GRP_CODE=T3.GRP_CODE")
				  .append(" AND T3.POL_CODE=T2.POL_CODE")
				  .append(" AND T2.GRP_CODE=T1.GRP_CODE")
				  .append(" AND T3.DIC_DATATYPE NOT IN ('BYTEA', 'XML', 'TEXT')")
				  .append(" ORDER BY T3.DIC_COLID ASC");
				  
				  lobQuery.append(" SELECT *")
				  .append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				  .append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				  .append(" AND T4.POL_CODE='"+pol_name+"'") 
				  .append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				  .append(" AND T4.TB_NAME='"+dicTname+"'") 
				  .append(" AND T1.GRP_TYPE='S'")
				  .append(" AND T3.DIC_CLONE_YN='Y'")
				  .append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				  .append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				  .append(" AND T4.POL_CODE=T3.POL_CODE")
				  .append(" AND T4.GRP_CODE=T3.GRP_CODE")
			  	  .append(" AND T3.POL_CODE=T2.POL_CODE")
			  	  .append(" AND T2.GRP_CODE=T1.GRP_CODE")
			  	  .append(" AND T3.DIC_DATATYPE IN ('BYTEA', 'XML', 'TEXT')")
			  	  .append(" ORDER BY T3.DIC_COLID ASC");
				 
				
			} else if ( XLConf.DBTYPE_SRC == XLCons.MSSQL ) {
				// gssg - ms2ms ����
				// gssg - lob Ÿ�� ���� ó��
				  query.append("SELECT *")
				  .append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				  .append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				  .append(" AND T4.POL_CODE='"+pol_name+"'") 
				  .append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				  .append(" AND T4.TB_NAME='"+dicTname+"'") 
				  .append(" AND T1.GRP_TYPE='S'")
				  .append(" AND T3.DIC_CLONE_YN='Y'")
				  .append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				  .append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				  .append(" AND T4.POL_CODE=T3.POL_CODE")
				  .append(" AND T4.GRP_CODE=T3.GRP_CODE")
			  	  .append(" AND T3.POL_CODE=T2.POL_CODE")
			  	  .append(" AND T2.GRP_CODE=T1.GRP_CODE")
			  	  .append(" AND T3.DIC_DATATYPE NOT IN ('TEXT', 'NTEXT', 'IMAGE', 'XML')")
			  	  .append(" ORDER BY T3.DIC_COLID ASC");
		
				  lobQuery.append("SELECT *")
				  .append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				  .append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				  .append(" AND T4.POL_CODE='"+pol_name+"'") 
				  .append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				  .append(" AND T4.TB_NAME='"+dicTname+"'") 
				  .append(" AND T1.GRP_TYPE='S'")
				  .append(" AND T3.DIC_CLONE_YN='Y'")
				  .append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				  .append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				  .append(" AND T4.POL_CODE=T3.POL_CODE")
				  .append(" AND T4.GRP_CODE=T3.GRP_CODE")
			  	  .append(" AND T3.POL_CODE=T2.POL_CODE")
			  	  .append(" AND T2.GRP_CODE=T1.GRP_CODE")
			  	  .append(" AND T3.DIC_DATATYPE IN ('TEXT', 'NTEXT', 'IMAGE', 'XML')")
			  	  .append(" ORDER BY T3.DIC_COLID ASC");
				 
			}  else {
				// -- Query �Ǵ� index �����Ͽ� Ʃ�� �ʿ�
				query.append("SELECT *")
				.append(" FROM NR_GROUP T1, NR_POL T2, NR_DICINFO T3, NR_CLONETB T4")
				.append(" WHERE T4.GRP_CODE='"+grpCode+"'") 
				.append(" AND T4.POL_CODE='"+pol_name+"'") 
				.append(" AND T4.TB_OWNER='"+dicOwner+"'") 
				.append(" AND T4.TB_NAME='"+dicTname+"'") 
				.append(" AND T1.GRP_TYPE='S'")
				.append(" AND T3.DIC_CLONE_YN='Y'")
				.append(" AND T4.TB_NAME=T3.DIC_TNAME") 
				.append(" AND T4.TB_OWNER=T3.DIC_OWNER") 
				.append(" AND T4.POL_CODE=T3.POL_CODE")
				.append(" AND T4.GRP_CODE=T3.GRP_CODE")
		  	    .append(" AND T3.POL_CODE=T2.POL_CODE")
		  		.append(" AND T2.GRP_CODE=T1.GRP_CODE")
		  		.append(" ORDER BY T3.DIC_COLID ASC");
			}
			
			// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
			// gssg -xl lob Ÿ�� ����
			// gssg - LOB �÷� ���� ������ ó��			
			// gssg - xl p2p ����
			Vector vt = xlQuery.getList(_conn, query.toString());
			
			if(lobQuery.length() != 0) {
				Vector lobVt = xlQuery.getList(_conn, lobQuery.toString());				
				for ( int i=0; i<lobVt.size(); i++ ) {
					vt.add(lobVt.get(i));
				}
			}
			
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] getJobRunPolInfo sql = " + query.toString());
				XLLogger.outputInfoLog("[DEBUG] getJobRunPolInfo lobSql = " + lobQuery.toString());
			}
			
			XLLogger.outputInfoLog("[DEBUG] getJobRunPolInfo sql = " + query.toString());
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[EXCEPTION] Failed to get JobRunPolInfo information to Run.");
			} else {
				// XLLogger.outputInfoLog("The number of JobRunPolInfo information to Run = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) {
			
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
			
		}
	}
	
	// JOB�� �������� XL_REPORT ���̺� insert
		public boolean insertJobResultReport(Connection _conn, XLJobRunPol _jobRunPol, long _totalCommitCnt)
		{
			PreparedStatement pstmt = null;
			try {
				
				StringBuffer query = new StringBuffer();
				
				// gssg - xl īŻ�α� mariadb
				query.append("INSERT INTO XL_REPORT(");
				if(XLConf.DBTYPE_STR == XLCons.PPAS) {
					query.append("REPORT_SEQ,");              
					
				}
						query.append("REPORT_POLNAME,")         
						.append("REPORT_SDB,")             
						.append("REPORT_SDB_TYPE,")        
						.append("REPORT_TDB,")             
						.append("REPORT_TDB_TYPE,")
						
						.append("REPORT_SOWNER,")          
						.append("REPORT_STABLE,")          
						.append("REPORT_TOWNER,")          
						.append("REPORT_TTABLE,")          
						
						.append("REPORT_CONDITION_SEQ,")   
						.append("REPORT_CONDITION_WHERE,") 
						.append("REPORT_COMMIT_CNT,")      
						.append("REPORT_SCHEDNAME,")       
						
						.append("REPORT_SDATE,")           
						
						.append("REPORT_EDATE,")  // SYSDATE    
						
						.append("REPORT_STATUS,")          
						.append("REPORT_PRIORITY,")        
						.append("REPORT_EVENT_MSG,")       
						.append("REPORT_DEL_YN,")          
						.append("REPORT_CDATE,") // SYSDATE          
						// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
						.append("REPORT_REGUSER, ")
						.append("REPORT_WORKPLAN_SEQ) ")
						;
				// gssg - xl īŻ�α� mariadb
				if(XLConf.DBTYPE_STR == XLCons.PPAS) {
					// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
					query.append("VALUES (SEQ_XL_REPORT.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,SYSDATE,?,?,?,?,SYSDATE,?, ?)");				
				}else {
					// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
					query.append("VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,NOW(),?,?,?,?,NOW(),?, ?)");								
				}
				
				// PreparedStatement ����
				pstmt = _conn.prepareStatement(query.toString());
				
				pstmt.setString(1, _jobRunPol.getPolName());
				
				// pstmt.setString(2, _jobRunPol.getSdbInfo().getIp());
				pstmt.setString(2, _jobRunPol.getSdbInfo().getIp() + "(" + _jobRunPol.getSdbInfo().getDbSid() + ")");
				pstmt.setString(3, _jobRunPol.getSdbInfo().getDbTypeStr());
				// pstmt.setString(4, _jobRunPol.getTdbInfo().getIp());
				pstmt.setString(4, _jobRunPol.getTdbInfo().getIp() + "(" + _jobRunPol.getTdbInfo().getDbSid() + ")");
				pstmt.setString(5, _jobRunPol.getTdbInfo().getDbTypeStr());
				
				pstmt.setString(6, _jobRunPol.getTableInfo().getSowner());
				pstmt.setString(7, _jobRunPol.getTableInfo().getStable());
				pstmt.setString(8, _jobRunPol.getTableInfo().getTowner());
				pstmt.setString(9, _jobRunPol.getTableInfo().getTtable());
				
				pstmt.setLong(10, _jobRunPol.getCondSeq());
				pstmt.setString(11, _jobRunPol.getCondWhere());
				pstmt.setLong(12, _totalCommitCnt);
				pstmt.setString(13, _jobRunPol.getSchedName());
				
				// pstmt.setString(14, XLUtil.getCurrentDateStr()); // REPORT_SDATE
				//pstmt.setString(14, _jobRunPol.getsDate()); // REPORT_SDATE
				// cksohn - catalog ppas
				// pstmt.setTimestamp(14, Timestamp.valueOf(_jobRunPol.getsDate()) );
				// cksohn - SCHED_ETIME üũ ���� ����
				
				// XLLogger.outputInfoLog("[DEBUG] _jobRunPol.getsDate() = " + _jobRunPol.getsDate());
				
				if (_jobRunPol.getsDate()== null || _jobRunPol.getsDate().equals("") ) {
					// pstmt.setNull(14, java.sql.Types.NULL);
					// cksohn - XL_CAP_READ_REDO_YN - RAC���� switch �� ������ ���� ����
					// getCurrentDateStr
					pstmt.setTimestamp(14, Timestamp.valueOf(XLUtil.getCurrentDateStr()) );
				} else {
					pstmt.setTimestamp(14, Timestamp.valueOf(_jobRunPol.getsDate()) );
				}
				
				
//				XLLogger.outputInfoLog("CKSOHN DEBUG-----------------" + _jobRunPol.getJobStatus());
				
				pstmt.setString(15, _jobRunPol.getJobStatus());
				
				pstmt.setInt(16, _jobRunPol.getPriority());
				if ( _jobRunPol.getJobStatus().equals(XLOGCons.STATUS_SUCCESS) ) {
					pstmt.setNull(17, java.sql.Types.NULL);
				} else {
					String errMsg = "";
					String recv_errMsg = _jobRunPol.getErrMsg_Recv();
					String apply_errMsg = _jobRunPol.getErrMsg_Apply();
					if ( recv_errMsg != null ) {
						errMsg += recv_errMsg;
					}
					if ( apply_errMsg != null ) {
						// if ( recv_errMsg != null ) errMsg += "\n";
						// cksohn - xl - ������ status log �� �α��ϵ���  - �α��� �Ϻ� ����
						if ( errMsg.length() > 0  ) errMsg += "\n";
						errMsg += apply_errMsg;
					}
					pstmt.setString(17, errMsg);
				}
				pstmt.setString(18, "N");
				pstmt.setNull(19, java.sql.Types.NULL);
				
				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
				pstmt.setLong(20, _jobRunPol.getWorkPlanSeq());
							
				pstmt.executeUpdate();
				// _conn.commit();
				
				return true;
				
			} catch (Exception e) {
				
				XLException.outputExceptionLog(e);
				return false;
				
			} finally {
				
				try { if ( pstmt != null ) pstmt.close(); } catch (Exception e) {} finally { pstmt = null; }
				
			}
			
		}

		
		// JOB�� ������  - CONDITION_ACTION Update
		public boolean updateJobResultCond(Connection _conn, XLJobRunPol _jobRunPol)
		{
			PreparedStatement pstmt = null;
			try {
				
				StringBuffer query = new StringBuffer();
				
				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
				query.append("UPDATE XL_CONDITION SET CONDITION_ACTION=?")
//					.append(" WHERE CONDITION_SEQ=? AND CONDITION_POLNAME=?");
					.append(" WHERE CONDITION_SEQ=? AND CONDITION_POLNAME=? AND CONDITION_WORKPLAN_SEQ=?");
				
				// PreparedStatement ����
				pstmt = _conn.prepareStatement(query.toString());
				
				pstmt.setString(1, _jobRunPol.getJobStatus());
				pstmt.setLong(2, _jobRunPol.getCondSeq());
				pstmt.setString(3, _jobRunPol.getPolName());			
				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
				pstmt.setLong(4, _jobRunPol.getWorkPlanSeq());			
							
				pstmt.executeUpdate();
				// _conn.commit();
				
				return true;
				
			} catch (Exception e) {
				
				XLException.outputExceptionLog(e);
				return false;
				
			} finally {
				
				try { if ( pstmt != null ) pstmt.close(); } catch (Exception e) {} finally { pstmt = null; }
				
			}
			
		}
		
		
		public boolean deleteJobQ(Connection _conn, XLJobRunPol _jobRunPol)
		{
			PreparedStatement pstmt = null;
			try {
				
				StringBuffer query = new StringBuffer();
				
				query.append("DELETE FROM XL_JOBQ")
					.append(" WHERE JOBQ_SEQ=?");
				
				// PreparedStatement ����
				pstmt = _conn.prepareStatement(query.toString());
				
				//pstmt.setLong(1, _jobRunPol.getJobseq());
				pstmt.executeUpdate();
				// _conn.commit();
				
				return true;
				
			} catch (Exception e) {
				
				XLException.outputExceptionLog(e);
				return false;
				
			} finally {
				
				try { if ( pstmt != null ) pstmt.close(); } catch (Exception e) {} finally { pstmt = null; }
				
			}
			
		}

		
		// ���� JOBQ �� 'W'�� �ƴ� ���·� ���� �ִ� JOBQ ���� ����
		// MGR ���� ������ ���� �ִ°� ������ FAIL ó�� �ϱ� ���� �ַ� ���
		public Vector getRJobQToFail() {
			
			try {
				XLQuery xlQuery = getXLQuery();
				StringBuffer query = new StringBuffer();
				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//				query.append("SELECT * FROM XL_JOBQ WHERE JOBQ_STATUS<>'W'");
				query.append("SELECT * FROM XL_JOBQ T1, XL_WORKPLAN T2 WHERE T1.JOBQ_STATUS<>'W'")
				.append(" AND T2.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
				.append(" AND T1.JOBQ_WORKPLAN_SEQ=T2.WORKPLAN_SEQ");
				
				Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());			
				
				if ( vt == null ) {
					XLLogger.outputInfoLog("[EXCEPTION][getRJobQToFail] Failed to get JobQ Running Job information.");
				} 
				
				return vt;
				
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		
		// ���� JOBQ �� 'W'  ���·� ���� �ִ� JOBQ ���� ����
		// MGR ���� ������ ���� �ִ°� ������ CANCEL ó�� �ϱ� ���� �ַ� ���
		public Vector getWJobQToCancel() {
			
			try {
				XLQuery xlQuery = getXLQuery();
				StringBuffer query = new StringBuffer();
				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//				query.append("SELECT * FROM XL_JOBQ WHERE JOBQ_STATUS='W'");			
				query.append("SELECT * FROM XL_JOBQ T1, XL_WORKPLAN T2 WHERE T1.JOBQ_STATUS='W'")
				.append(" AND T2.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
				.append(" AND T1.JOBQ_WORKPLAN_SEQ=T2.WORKPLAN_SEQ");
				
				Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());			
				
				if ( vt == null ) {
					XLLogger.outputInfoLog("[EXCEPTION][getWJobQToCancel] Failed to get JobQ Waiting Job information.");
				} 
				
				return vt;
				
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		
		// JOBQ�� 'W' �۾��� SCHED_ETIME �� �����ų�, 
		//       'R'   "  SCHED_ITIME �� �����۾� ����
		//  --> CANCEL �Ǵ� ABORT ��� �۾�
		public Vector getJobToCancelOrAbort(Connection _conn) {
			
			try {
				
				String curDateStr = XLUtil.getCurrentDateStr();
				
				XLQuery xlQuery = getXLQuery();
				StringBuffer query = new StringBuffer();
				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//				query.append("SELECT * FROM XL_JOBQ") 
//						.append(" WHERE (JOBQ_STATUS='W' AND JOBQ_SCHED_ETIME IS NOT NULL AND JOBQ_SCHED_ETIME < '" + curDateStr + "')") 
//						.append(" OR (JOBQ_STATUS='R' AND JOBQ_SCHED_ITIME IS NOT NULL AND JOBQ_SCHED_ITIME < '" + curDateStr + "')"); 
				query.append("SELECT * FROM XL_JOBQ T1, XL_WORKPLAN T2") 
				.append(" WHERE " + "(" + "(T1.JOBQ_STATUS='W' AND T1.JOBQ_SCHED_ETIME IS NOT NULL AND T1.JOBQ_SCHED_ETIME < '" + curDateStr + "')") 
				.append(" OR (T1.JOBQ_STATUS='R' AND T1.JOBQ_SCHED_ITIME IS NOT NULL AND T1.JOBQ_SCHED_ITIME < '" + curDateStr + "')" + ")")
				.append(" AND T2.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
				.append(" AND T1.JOBQ_WORKPLAN_SEQ=T2.WORKPLAN_SEQ");
							
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] getJobToCancelOrAbort : " + query.toString());
				}

				
				// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
				Vector vt = xlQuery.getList(_conn, query.toString());
				

				if ( vt == null ) {
					XLLogger.outputInfoLog("[EXCEPTION] Failed to get getJobToCancelOrAbort information.");
				} else {
					// XLLogger.outputInfoLog("The number of Running job = " + vt.size());
				}
				
				return vt;
				
			} catch (Exception e) {
				// e.printStackTrace();
				XLException.outputExceptionLog(e);
				return null;
			}
		}
		
		/**
		 * SELECT T1.POL_TOWNER, T1.POL_TTABLE,  T2.CONDITION_WHERE, , T2.CONDITION_KEY,
				T3.DBMS_IP, T3.DBMS_SID 
				FROM XL_POL T1, XL_CONDITION T2, XL_DBMS T3
				WHERE 
				T1.POL_NAME='POL_001'
				AND T2.CONDITION_WHERE='ID<=3000'
				AND T1.POL_NAME=T2.CONDITION_POLNAME
				AND T3.DBMS_SEQ=T1.POL_TDB_SEQ;
		 */
		// cksohn - XL_DELETE_TARGET/XL_TRUNCATE_TARGET ��� �߰�
		// ����ڿ� ���� ��������� Ÿ�� ���̺� delete or truncate ��û���� delete/truncate�� ���� �⺻ ���� ����
		public Vector getDeleteTargetInfo(String _polName, Vector _vtCondWhere) {
			
			try {
				
				XLQuery xlQuery = getXLQuery();
				StringBuffer query = new StringBuffer();

				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//				query.append("SELECT T1.POL_TOWNER, T1.POL_TTABLE, T2.CONDITION_WHERE, T2.CONDITION_KEY,")
//						.append(" T3.DBMS_IP, T3.DBMS_SID") // Target DB ���� 
//						.append(" FROM XL_POL T1, XL_CONDITION T2, XL_DBMS T3")
//						.append(" WHERE T1.POL_NAME='" + _polName + "'");
				query.append("SELECT T1.POL_TOWNER, T1.POL_TTABLE, T2.CONDITION_WHERE, T2.CONDITION_KEY,")
				.append(" T3.DBMS_IP, T3.DBMS_SID") // Target DB ���� 
				.append(" FROM XL_POL T1, XL_CONDITION T2, XL_DBMS T3, XL_WORKPLAN T4")
				.append(" WHERE T1.POL_NAME='" + _polName + "'");

				
				
						if ( _vtCondWhere != null ) {
							// truncate ����� ���� �ʿ� ����, ���Ǻ� delete ����ÿ��� ���� �߰�
							query.append(" AND T2.CONDITION_WHERE IN (");
							for (int i=0; i<_vtCondWhere.size(); i++) {
								if ( i != 0 ) {
									query.append(",");
								}
								// gssg - ���������ڿ������� �������̰� ���
								// gssg - Ÿ�� delete ����� �̱� ���� ���� ����
//								query.append("'" + (String)_vtCondWhere.get(i) + "'");
								query.append("'" + (String)_vtCondWhere.get(i).toString().replace("'", "''") + "'");
							}
							
							query.append(")");
						}
						
						query.append(" AND T1.POL_NAME=T2.CONDITION_POLNAME")
						.append(" AND T3.DBMS_SEQ=T1.POL_TDB_SEQ");

						// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
						query.append(" AND T4.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
						.append(" AND T1.POL_WORKPLAN_SEQ=T4.WORKPLAN_SEQ AND T2.CONDITION_WORKPLAN_SEQ=T4.WORKPLAN_SEQ AND T3.DBMS_WORKPLAN_SEQ=T4.WORKPLAN_SEQ");

						
				// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
				Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
				
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] getDeleteTargetInfo sql = " + query.toString());
				}
				
				if ( vt == null ) {
					XLLogger.outputInfoLog("[EXCEPTION] Failed to get Target DB information to delete/truncate table.");
				} else {
					// XLLogger.outputInfoLog("The number of JobRunPolInfo information to Run = " + vt.size());
				}
				
				return vt;
				
			} catch (Exception e) {
				
				// e.printStackTrace();
				XLException.outputExceptionLog(e);
				return null;
				
			}
		}
		
		
		// cksohn - XL_DELETE_SOURCE / XL_PART_DROP_SOURCE / XL_PART_TRUNC_SOURCE
		public Vector getDeleteSourceInfo(String _polName, Vector _vtCondWhere) {
			
			try {
				
				XLQuery xlQuery = getXLQuery();
				StringBuffer query = new StringBuffer();

				// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
//				query.append("SELECT T1.POL_SOWNER, T1.POL_STABLE, T2.CONDITION_WHERE, T2.CONDITION_KEY,")
//						.append(" T3.DBMS_IP, T3.DBMS_SID") // Target DB ���� 
//						.append(" FROM XL_POL T1, XL_CONDITION T2, XL_DBMS T3")
//						.append(" WHERE T1.POL_NAME='" + _polName + "'");
				query.append("SELECT T1.POL_SOWNER, T1.POL_STABLE, T2.CONDITION_WHERE, T2.CONDITION_KEY,")
				.append(" T3.DBMS_IP, T3.DBMS_SID") // Target DB ���� 
				.append(" FROM XL_POL T1, XL_CONDITION T2, XL_DBMS T3, XL_WORKPLAN T4")
				.append(" WHERE T1.POL_NAME='" + _polName + "'");

				
						if ( _vtCondWhere != null ) {
							// truncate ����� ���� �ʿ� ����, ���Ǻ� delete ����ÿ��� ���� �߰�
							query.append(" AND T2.CONDITION_WHERE IN (");
							for (int i=0; i<_vtCondWhere.size(); i++) {
								if ( i != 0 ) {
									query.append(",");
								}
								// gssg - ���������ڿ������� �������̰� ���
								// gssg - Ÿ�� delete ����� �̱� ���� ���� ����
//								query.append("'" + (String)_vtCondWhere.get(i) + "'");
								query.append("'" + (String)_vtCondWhere.get(i).toString().replace("'", "''") + "'");
							}
							
							query.append(")");
						}
						
						query.append(" AND T1.POL_NAME=T2.CONDITION_POLNAME")
						.append(" AND T3.DBMS_SEQ=T1.POL_SDB_SEQ");

						// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
						query.append(" AND T4.WORKPLAN_NAME='" + XLConf.XL_WORKPLAN_NAME + "'")
						.append(" AND T1.POL_WORKPLAN_SEQ=T4.WORKPLAN_SEQ AND T2.CONDITION_WORKPLAN_SEQ=T4.WORKPLAN_SEQ AND T3.DBMS_WORKPLAN_SEQ=T4.WORKPLAN_SEQ");

				
				// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
				Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
				
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] getDeleteSourceInfo sql = " + query.toString());
				}
				
				if ( vt == null ) {
					XLLogger.outputInfoLog("[EXCEPTION] Failed to get Source DB information to delete/truncate table.");
				} else {
					// XLLogger.outputInfoLog("The number of JobRunPolInfo information to Run = " + vt.size());
				}
				
				return vt;
				
			} catch (Exception e) {
				
				// e.printStackTrace();
				XLException.outputExceptionLog(e);
				return null;
				
			}
		}

	
	// gssg - LG���� MS2O
	// gssg - �����ڵ� �� ó��
	public Vector getCustomCode(String _polName) {
		
		try {
			
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();

			
			query.append("SELECT T1.DICINFO_COLNAME, T2.DICCODE_VALUE")
					.append(" FROM XL_DICINFO_CT T1, XL_DICCODE T2")
					.append(" WHERE T1.DICINFO_CODETYPE = T2.DICCODE_TYPE")
					.append(" AND T1.DICINFO_POLNAME ='" + _polName + "'");
			
			
			// Vector vt = xlQuery.getList(XLConf.XLM_DBTYPE_STR, query.toString());
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] getCustomCode sql = " + query.toString());
			}
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[EXCEPTION] Failed to get Custom Code.");
			} else {
				// XLLogger.outputInfoLog("The number of JobRunPolInfo information to Run = " + vt.size());
			}
			
			return vt;
			
		} catch (Exception e) {
			
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			return null;
			
		}
	}
	

	
}
