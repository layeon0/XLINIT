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
	
	
	// cksohn - scheduler 1.0 최초 버전 수정  - session date format 
	
//	public static final String DATE_FORMAT_ALTI = "YYYY-MM-DD HH24:MI:SS";	// ALTIBASE date type 날짜 포멧
//	public static final String TIMESTAMP_FORMAT_ALTI = "YYYY-MM-DD HH24:MI:SS.SSSSSS";	// ALTIBASE TIMESTAMP 포멧
//	// cksohn - for sundb
//	public static final String DATE_FORMAT_SUNDB = "YYYY-MM-DD HH24:MI:SS";	
//	public static final String TIMESTAMP_FORMAT_SUNDB = "YYYY-MM-DD HH24:MI:SS.FF6";	
//	public static final String DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS";	// 오라클 date type 날짜 포멧
//	//3.2.00-015 2011-07-11 modify for nls_timestamp format
//	public static final String TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SSXFF";	// 오라클 TIMESTAMP 포멧
//	// sykim 3.2.01-037 2013-03-06 for timestamp zone format
//	public static final String TIMESTAMP_TZ_FORMAT = "YYYY-MM-DD HH24:MI:SSxFF TZH:TZM";
	
//------------------------------------------------------------------------------------
// DB 접속 및 쿼리 수행 관련 - START
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
							XLConf.DBTYPE_STR == XLCons.TIBERO ) { // cksohn - for Mysql Catalog DB  - cksohn - catalog db connection for mysql - connection 부분수정 & sysdate & rownum
		    			// date format 설정
		    			Statement st = con.createStatement();		
		    			st.executeQuery("alter session set nls_date_format='" + XLDBCons.DATE_FORMAT + "'");
		    			try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
	
						//3.2.00-015 2011-07-11 modify for nls_timestamp format
						st = con.createStatement();		
						st.executeQuery("alter session set nls_timestamp_format='" + XLDBCons.TIMESTAMP_FORMAT + "'");
						try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
						// cksohn- tibero4 nls_timestamp_tz_format 이슈 - comment
						//sykim 3.2.01-037 2013-03-06 for timestamp zone format
//						st = con.createStatement();		
//						st.executeQuery("alter session set nls_timestamp_tz_format='" + NRAPManager.TIMESTAMP_TZ_FORMAT + "'");
//						try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					}		
										
	    			break;	    			
				} catch(Exception e){
					// gssg - Troubleshooting 에러 코드 생성
					// gssg - 국가정보자원관리원 데이터이관 사업
					XLLogger.outputInfoLog("[E1001] XL Catalog Connection failed!! DB reconnection attempt("+ (i+1) +")");
					try { if(con != null) con.close(); } catch(Exception e1) {} finally { con = null; }
					Thread.sleep(10000);
				}
				
//				NRAPManager.outputInfoLog("CKSOHN DEBUG>>>>>>>>> isApplyError = " + NRAPManager.isApplyError);
				
//				//sykim 3.2.01-023 2012-09-19 for db connect retry
//				//DB connection 비정상 적일 경우 / 데몬 종료 요청 시 connection retry break
				//if(NRAPManager.isApplyError)break; // cksohn - scheduler 1.0 최초 버전 수정 - comment
				
			} // for-end			
		} catch (Exception e) {
			// cksohn - connection 부분 simple Exception 메시지 출력
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
	 * executeQuery - Select 결과를 넘겨줌 
	 * XLQuery에서 Long Type은 처리를 못해주는 듯 하다.
	 * 그래서 별도로 만듬
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
	
	// cksohn 3.3.00-001 2014-12-25 for DDL Create Table & DDL - 이쪽으로 
	/**
	 * executeQuery - Select 결과를 넘겨줌 
	 * XLQuery에서 Long Type은 처리를 못해주는 듯 하다.
	 * 그래서 별도로 만듬
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
// DB 접속 및 쿼리 수행 관련 - END
//------------------------------------------------------------------------------------
	
	
//------------------------------------------------------------------------------------
// NR_SVR , NR_DBMS , NR_GRP , NR_POL - START
//------------------------------------------------------------------------------------	


	// Sched port 정보 추출
	// 비정상 추출시 : return -1
	public int getSchedPort() {
		
		try {
			XLQuery nrQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			query.append("SELECT CONF_VALUE FROM XL_CONF WHERE CONF_OPTION='XL_SCHED_PORT'");
			
			Vector vt = nrQuery.getList(XLConf.DBTYPE_STR, query.toString());
			if ( vt == null || vt.size() == 0 ) {
				// gssg - Troubleshooting 에러 코드 생성
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
	
	// cksohn - scheduler 1.0 최초 버전 수정
	// XLConf 에서 conf 값 read
	public Vector getConfValues() {
		
		try {
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			// gssg - 일본 네트워크 분산 처리
//			query.append("SELECT CONF_OPTION, CONF_VALUE FROM XL_CONF");
			query.append("SELECT T1.CONF_OPTION, T1.CONF_VALUE") 
			.append(" FROM XL_CONF T1, XL_WORKPLAN T2") 
			.append(" WHERE T1.CONF_WORKPLAN_SEQ = T2.WORKPLAN_SEQ")
			.append(" AND T2.WORKPLAN_NAME = '" + XLConf.XL_WORKPLAN_NAME + "'");

			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
			// if ( vt == null || vt.size() == 0 ) {
			if ( vt == null || vt.size() == 0 ) {
				// gssg - Troubleshooting 에러 코드 생성
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
	
	// cksohn - scheduler 1.0 최초 버전 수정
	// catalog db로 부터 활성화 스케쥴 정보 추출
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
				// gssg - Troubleshooting 에러 코드 생성
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
	
	// 스케쥴 JOB 수행 대상 추출
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
					.append(" AND C.CONDITION_ACTION<>'S'") // 미수행, 중지, 실패, 취소 작업 모두 추출
					// gssg - 모듈 보완
					// gssg - 우선순위 수정
//					.append(" ORDER BY B.POL_PRIORITY DESC, B.POL_NAME ASC, C.CONDITION_SEQ ASC");
					.append(" ORDER BY B.POL_PRIORITY ASC, B.POL_NAME ASC, C.CONDITION_SEQ ASC");
			
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());
			
//			if ( vt == null || vt.size() == 0 ) {
//				XLLogger.outputInfoLog("[EXCEPTION] Failed to get conf information.");
//				return null;
//			}
			if ( vt == null ) {
				// gssg - Troubleshooting 에러 코드 생성
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

	// cksohn - scheduler 1.0 최초 버전 수정 JOBQ 등록 PreparedStament 생성
	public PreparedStatement getPstmtInsertJobQ(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		query.append("INSERT INTO XL_JOBQ(JOBQ_SEQ, JOBQ_POLNAME, JOBQ_CONDITION_WHERE, JOBQ_STATUS, JOBQ_PRIORITY, JOBQ_CDATE)") 
				.append(" VALUES (SEQ_XL_JOBQ.NEXTVAL, ?, ?, 'W', ?, SYSDATE)");
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
		}catch(Exception e){
			// cksohn - connection 부분 simple Exception 메시지 출력
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());

		}		
		return pstmt;
	}
	
	//ay - jobq insert 테스트
	public boolean insertJobQ(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		query.append("INSERT INTO XL_JOBQ( JOBQ_SDBIP,JOBQ_SDBSID,JOBQ_TDBIP,JOBQ_TDBSID,JOBQ_POLNAME,JOBQ_CONDITION_WHERE,JOBQ_COMMIT_CNT,JOBQ_SDATE,JOBQ_EDATE,JOBQ_STATUS,JOBQ_PRIORITY,JOBQ_CDATE,JOBQ_CONDITION_KEY,JOBQ_SCHED_STIME,JOBQ_SCHED_ETIME,"
				+ "JOBQ_SCHED_ITIME,JOBQ_WORKPLAN_SEQ)") 
				.append(" VALUES ( '172.17.0.4','XIDLDB','172.17.0.3' ,'XIDLDB','m2m','NONE',0,SYSDATE(),SYSDATE(),'W',3,SYSDATE(),'m2m',"
						+ "NULL,NULL,NULL,1)");
		XLLogger.outputInfoLog("[query] " + query.toString());
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
			pstmt.executeUpdate();
			return true;
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			return false;
			
		} finally {
			
			try { if ( pstmt != null ) pstmt.close(); } catch (Exception e) {} finally { pstmt = null; }
			
		}
	
	}
	
	// update JobQStatus PreparedStatement
	public PreparedStatement getPstmtUpdateJobQStatus(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		query.append("UPDATE XL_JOBQ SET JOBQ_STATUS=?, JOBQ_SDATE=? WHERE JOBQ_SEQ=? AND JOBQ_POLNAME=?");
				
		
		try{
			XLLogger.outputInfoLog("[getPstmtUpdateJobQStatus] " + query.toString());
			pstmt = _conn.prepareStatement(query.toString());
			
		}catch(Exception e){
			// cksohn - connection 부분 simple Exception 메시지 출력
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
			// cksohn - connection 부분 simple Exception 메시지 출력
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());
			return null;

		}		
		return pstmt;
	}
	
	// update CONDITION commit cnt PreparedStatement
	public PreparedStatement getPstmtUpdateCondCommitCnt(Connection _conn){
		PreparedStatement pstmt = null;
		
		StringBuffer query = new StringBuffer();
		// gssg - 일본 네트워크 분산 처리
//		query.append("UPDATE XL_CONDITION SET CONDITION_COMMIT_CNT=? WHERE CONDITION_POLNAME=? AND CONDITION_SEQ=?");
		query.append("UPDATE XL_CONDITION SET CONDITION_COMMIT_CNT=? WHERE CONDITION_POLNAME=? AND CONDITION_SEQ=? AND CONDITION_WORKPLAN_SEQ=?");
				
		
		try{
			pstmt = _conn.prepareStatement(query.toString());
			
		}catch(Exception e){
			// cksohn - connection 부분 simple Exception 메시지 출력
			XLLogger.outputInfoLog("[EXCEPTION] " + e.toString());
			return null;

		}		
		return pstmt;
	}
	

	// XL_DBMS 테이블 정보 추출
	// public Vector getDbmsInfo() {
	public Vector getDbmsInfo() {
		
		try {
		
			XLQuery xlQuery = getXLQuery();
			StringBuffer query = new StringBuffer();
			
			// gssg - 일본 네트워크 분산 처리
			query.append("SELECT * FROM NR_SVR T1, NR_DBMS T2, XL_CHARENCODE T3")
				 .append(" WHERE T1.SVR_SEQ=T2.SVR_SEQ")
				 .append(" AND T2.DBMS_SEQ=T3.ENCODE_DBSEQ");
			
			Vector vt = xlQuery.getList(XLConf.DBTYPE_STR, query.toString());	
						
			if ( vt == null ) {
				// gssg - Troubleshooting 에러 코드 생성
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
	

	// ayzn - Source, Target(일대다 정책일 시 NR_POL의 DBMS_SEQ 기준) 정보 추출 
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
				// gssg - Troubleshooting 에러 코드 생성
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

	// 이 구문은 상황에 따라 구문 길이가 4000 byte 이상이 될수도 있으므로, 
	// XLQuery를 사용하지 않고, PreparedStatement 구문으로 만들어서 XLQuery와 동일한 형태로 return 해 주도록 한다.
	// ayzn - Source DB 정보
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
	
			// PreparedStatement 생성
			pstmt = _conn.prepareStatement(query.toString());
			
			XLLogger.outputInfoLog("[Source INFO] [getInfo sql::]"+query.toString());
		
			// Query 수행
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
	
	// ayzn - 테이블 및 컬럼정보 
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
			// gssg - m2m LOB 컬럼 순서 끝으로 처리
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
				// gssg - xl p2p 지원
				// gssg - p2p LOB 컬럼 순서 끝으로 처리
				// gssg - xl 전체적으로 보완2
				// gssg - PostgreSQL 커넥터 처리					

				
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
				// gssg - ms2ms 지원
				// gssg - lob 타입 순서 처리
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
				// -- Query 또는 index 조정하여 튜닝 필요
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
			// gssg -xl lob 타입 보완
			// gssg - LOB 컬럼 순서 끝으로 처리			
			// gssg - xl p2p 지원
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

	
	// gssg - LG엔솔 MS2O
	// gssg - 공장코드 값 처리
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
