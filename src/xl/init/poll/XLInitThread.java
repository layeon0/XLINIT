package xl.init.poll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

import xl.lib.common.XLCons;
import xl.init.conf.XLConf;
import xl.init.dbmgr.XLMDBManager;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLMemInfo;
import xl.init.logger.XLLogger;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

/**
 * 
 * @author cksohn
 * 
 * X-LOG 스케쥴러가 주기적으로 XL_SCHED_POLLING_INT=3600 sec 마다 한번씩 
 * catalog db 에 접속해서 변경된 스켸쥴 정보가 있는지 check하고 갱신
 *
 */
public class XLInitThread extends Thread {

	private String polCode="";
	private String grpCode="";
	private String tableName="";

	public XLInitThread()
	{
	}
	
	public XLInitThread(String _grpCode,String _polCode,String _tableName)
	{
		this.grpCode = _grpCode;
		this.polCode = _polCode;
		this.tableName = _tableName;	
	}
	
	@Override
	public void run(){

		XLLogger.outputInfoLog("X-LOG init thread is started.");
		
		Connection cataConn = null;
		
		XLMDBManager mDBMgr = new XLMDBManager();

		if ( XLInit.STOP_FLAG ) {
			XLLogger.outputInfoLog(" init thread thread is stopped - stop request");
			return;
		}
		
		// 메모리 초기화
		XLMemInfo.HT_JOBQ_DBMS_TMP.clear();

		// 여기서는 여러 Query를 수행해야 하므로, 주기마다 Connection 을 생성하고 재사용한후 , close 한다. 
		cataConn = mDBMgr.createConnection(false);
		Vector vt = null;
		try {
			
			String dicOwner = "";
			String dicTname = "";
			
			StringTokenizer tokenizer = new StringTokenizer(this.tableName, ".");
			while(tokenizer.hasMoreTokens()){            
	            if( tokenizer.hasMoreTokens() ) dicOwner = tokenizer.nextToken().trim();
				if( tokenizer.hasMoreTokens() ) dicTname = tokenizer.nextToken().trim();
			} 
			
			XLLogger.outputInfoLog("grpCode = " + this.grpCode);
			XLLogger.outputInfoLog("polCode = " +this.polCode);
			XLLogger.outputInfoLog("SOURCE dicOwner = " + dicOwner);
			XLLogger.outputInfoLog("SOURCE dicTname = " + dicTname);
				
			Vector vt_info = mDBMgr.getSourceInfo(cataConn, this.grpCode, this.polCode, dicOwner, dicTname);
							
			//if ( vt_info == null) {
			// ayzn - size < 1 체크 추가
			if ( vt_info == null || vt_info.size() < 1){
				XLLogger.outputInfoLog("[WARN] Failed to get information.. ");
				vt_info = new Vector(); // JOBQ에 데이터가 하나도 없는 것처럼 처리하기 위해.
				System.exit(0);
			}
		
			XLLogger.outputInfoLog(vt_info);
			 	
			// 대상 정책 정보 세팅
			for ( int i=0; i<vt_info.size(); i++ ) {
				Hashtable ht = (Hashtable)vt_info.get(i);
				
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
			
				String dbmsKey = sdbIp + "_" + sdbSid;
				
				XLLogger.outputInfoLog("[dbmsKey]"+dbmsKey);
				 
				// check dbms 별 동시 max running job 갯수
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				
				if ( dbmsInfo == null ) {
					
					XLLogger.outputInfoLog("[WARN] Cannot find source DBMS infomation - " + sdbIp + "/" + sdbSid);
					XLLogger.outputInfoLog("[WARN] Please Check X-LOG DBMS Information!!!");
					continue;
					
				}
			
				// JOBQ 수행 대상 DBMS 정보 등록
				XLMemInfo.HT_JOBQ_DBMS_TMP.put(sdbIp, sdbSid);
			} // for-end
			
			
			// 해당 정책
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog("####  runPol START  ####");
			
			
			// 4. 3번의 HT_JOBQ_POL_CNT_TMP에 등록된 대상 정책들에 대한 조건체크후 JOB 수행
			if ( !runPol(cataConn,vt_info) ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Pol ...");
				System.exit(0);
			}				
					
			XLLogger.outputInfoLog("");

			
		} catch(Exception e) {
			
			XLException.outputExceptionLog(e);
			
		} finally {
			
			try { if ( cataConn != null ) cataConn.close(); } catch (Exception e) {} finally { cataConn = null; }
			
		}
	}
	
	// job 수행
	private boolean runPol(Connection _cataConn,Vector vt_info)
	{
	
		PreparedStatement pstmt_updateStatus = null;
		
		try {

			XLMDBManager mDBMgr = new XLMDBManager();
			
			//XLLogger.outputInfoLog(vt_info);
		
			if ( vt_info == null ) {
				return false;
			}
			
			// source db 정보 세팅
			for (int i=0; i<vt_info.size(); i++) {
				
				Hashtable ht = (Hashtable)vt_info.get(i);
	
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
				
				String dbmsKey = sdbIp + "_" + sdbSid;
				
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				
				// 조건설정 (nr_clonetb 테이블)
				String condWhere = (String)ht.get("TB_CONDITION");
				
				XLLogger.outputInfoLog("  condWhere : ["+condWhere +"]");
				XLLogger.outputInfoLog("");
				
				
				XLJobRunPol jobRunPolInfo = new XLJobRunPol(grpCode, polCode, tableName, condWhere);
				
				if ( !jobRunPolInfo.makeInfo(_cataConn) ) {
					XLLogger.outputInfoLog("[WARN] Failed to Run Job policy - " + polCode);
					return false;
				}
				
				XLMemInfo.addRJobPolInfo(polCode, jobRunPolInfo);
				jobRunPolInfo.setsDate(XLUtil.getCurrentDateStr());

				// JOB 수행

				// cksohn - BULK mode oracle sqlldr
				// jobRunPolInfo.exeJob();
				// if ( jobRunPolInfo.getExeMode() == XLCons.NORMAL_MODE && jobRunPolInfo.getTdbInfo().getDbType() != XLCons.ORACLE ) {
				// gssg - xl o2p bulk mode 지원
				// gssg - ppas bulk thread 추가
				// gssg - xl t2t 지원
				// gssg - t2t bulk mode 지원
				if  ( jobRunPolInfo.getTdbInfo().getDbType() == XLCons.ORACLE || 
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.MARIADB || 
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.MYSQL ||
						// gssg - xl 전체적으로 보완2
						// gssg - PostgreSQL 커넥터 처리
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.PPAS || 
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.POSTGRESQL ) {
					
					if ( jobRunPolInfo.getExeMode() == XLOGCons.NORMAL_MODE ) { // cksohn - XL_BULK_MODE_YN conf 값 설정
						XLLogger.outputInfoLog("[NOMAL MODE]");
						jobRunPolInfo.exeJob();
						
					} else if ( jobRunPolInfo.getExeMode() == XLOGCons.BULK_MODE ) { // BULK MODE - 현재까지는 타겟 Oracle 만 지원
						XLLogger.outputInfoLog("[BULK MODE]");
						jobRunPolInfo.exeJobBulk();
						
					} else if ( jobRunPolInfo.getExeMode() == XLOGCons.LINK_MODE ) {
						
						jobRunPolInfo.exeJobLink();
					}
					
				} 
				else {
					
					// BULK 모드 지원 안하는 DB는 NORMAL_MODE로 수행 (추후 DBMS 별로 수정 필요)
					jobRunPolInfo.exeJob();
					
				}
				
				
			} // for-end
			
			
			return true;
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			return false;
			
		} finally {
			try { if ( pstmt_updateStatus != null ) pstmt_updateStatus.close(); } catch (Exception e) {} finally { pstmt_updateStatus = null; }
		}
	}
	

}
