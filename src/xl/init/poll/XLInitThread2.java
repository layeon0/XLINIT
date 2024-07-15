package xl.init.poll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Hashtable;
// ayzn - XLInit 기능 개발
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
 * @author ayzn
 * 
 * 기존 IDL XLJobPollingThread.java를    XLInitThread.java로 변경
 * IDL에서 사용하던 JOBQ,주기적으로 실행하는 Polling 관련 코드 제거 및  수정, 로그 수정
 *
 */
public class XLInitThread2 extends Thread {

	private String polCode="";
	private String grpCode="";
	private String tableName="";

	public XLInitThread2()
	{
	}
	
	public XLInitThread2(String _grpCode,String _polCode,String _tableName)
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
		
		// ayzn - XLInit 기능 개발  / initThread추가 : CNT관련 메모리 주석처리
		// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
		//XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.clear();
		
		// ayzn - XLInit 기능 개발  / initThread추가 : CNT관련 메모리 주석처리
		//XLMemInfo.HT_JOBQ_POL_CNT_TMP.clear();

		// 여기서는 여러 Query를 수행해야 하므로, 주기마다 Connection 을 생성하고 재사용한후 , close 한다. 
		cataConn = mDBMgr.createConnection(false);
		Vector vt = null;
		try {
			
			// ayzn - XLInit 기능 개발  / initThread추가 : CNT관련메모리 처리 주석, JOBQ 관련 함수 주석, CDC 카탈로그 참조 DB처리 변경
			/*
			 
			XLLogger.outputInfoLog("[JOBQ] Polling Check JobQ start");
			
			///// Running 중인 JOB 들중 작업 강제종료 시간이 지난 작업들은 여기서 ABORT
			////// W 작업중 ETIME 날자가 초과한 작업들은 CANCEL
			vt = mDBMgr.getJobToCancelOrAbort(cataConn);
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get getJobToCancelOrAbort information.. Retry next Job polling interval time");
				
			} else {
				if ( vt.size() > 1) {
					cancelOrAbortJob(cataConn, vt);
				}
				
			}

			
			// 1. JobQ에서  소스 DBMS 별로 Running 중인 JOB의 정보 추출
			vt = mDBMgr.getRJobCntByDbms(cataConn);
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get JobQ information.. Retry next Job polling interval time");
				vt = new Vector(); // JOBQ에 데이터가 하나도 없는 것처럼 처리하기 위해.
			}
			
			*/
			
			// ayzn - XLInit 기능 개발 - 1. 소스 DB 정보 세팅
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
							
			// ayzn - XLInit 기능 개발  / initThread추가 :  size < 1 체크 추가
			//if ( vt_info == null) {
			if ( vt_info == null || vt_info.size() < 1){
				XLLogger.outputInfoLog("[WARN] Failed to get information.. ");
				vt_info = new Vector(); // JOBQ에 데이터가 하나도 없는 것처럼 처리하기 위해.
				System.exit(0);
			}
		
			XLLogger.outputInfoLog(vt_info);
			 
			// ayzn - XLInit 기능 개발 : CNT관련메모리 처리 주석, JOBQ 관련 함수 주석, CDC 카탈로그 참조  DB처리 변경
			/* 
			
			// 2. dbms_sid별  Running 중인 job이 maxJobCnt를 초과하지 않는 것들만 1차 수행대상
			//    - 이때, Running중이 지 않은 job이 해당 소스 DBMS에서 수행되는 것이 것에 대한 cnt도 추출(0)
			for ( int i=0; i<vt.size(); i++ ) {
				
				Hashtable ht = (Hashtable)vt.get(i);
				String sdbIp = (String)ht.get("JOBQ_SDBIP");
				String sdbSid = (String)ht.get("JOBQ_SDBSID");
				
				// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
				int rCnt = Integer.parseInt((String)ht.get("RCNT"));
				
				String dbmsKey = sdbIp + "_" + sdbSid;
				// check dbms 별 동시 max running job 갯수
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				if ( dbmsInfo == null ) {
					
					XLLogger.outputInfoLog("[WARN] Cannot find source DBMS infomation - " + sdbIp + "/" + sdbSid);
					XLLogger.outputInfoLog("[WARN] Please Check X-LOG DBMS Information!!!");
					continue;
					
				}
				
				// if  ( rCnt >= dbmsInfo.getMaxJobCnt() ) {
				// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
				//int rCnt = htRCntDBMS.get(dbmsKey);
				if  ( rCnt >= dbmsInfo.getMaxJobCnt() ) {
					
					XLLogger.outputInfoLog("[JOBQ] Already Running max concurrent job - " + sdbIp + "/" + sdbSid);
					XLLogger.outputInfoLog("       Running Job : " + rCnt + " >= Concurrent Max Job : " + dbmsInfo.getMaxJobCnt()); 
					
				} else {
					
					// JOBQ 수행 대상 DBMS 정보 등록
					XLMemInfo.HT_JOBQ_DBMS_TMP.put(sdbIp, sdbSid);
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.put(sdbIp + "_" + sdbSid, rCnt);
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					//rCnt++;
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					//htRCntDBMS.put(dbmsKey, rCnt);
				}
				
			} // for-end
			
			*/
			
			// ayzn - XLInit 기능 개발  - 2. dbms_sid로 수행대상 등록
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
			
			// ayzn - XLInit 기능 개발 : CNT관련메모리 처리 주석, JOBQ 관련 함수 주석
			/*
			 
			// 3.HT_JOBQ_DBMS중 수행가능한 정책 정보 메모링 등록
			//   - 정책별 Runnung 중인 작업수(count : runPolJobCnt 추출해서 HTRUNNING_POL_CNT 에 등록
			//   - runPolJobCnt < tmaxJobCnt 인 것들만 작업수행 대상임
			vt = mDBMgr.getRJobCntByPol(cataConn);
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get JobQ policy information.. Retry next Job polling interval time");
				Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
				continue;
			}
			
			for ( int i=0; i<vt.size(); i++ ) {
									
				Hashtable ht = (Hashtable)vt.get(i);
				String polName = (String)ht.get("JOBQ_POLNAME");
				int polTMaxJobCnt = Integer.parseInt((String)ht.get("POL_TMAX_JOBCNT"));
				int polRJobCnt = Integer.parseInt((String)ht.get("RCNT"));
				
				if ( polRJobCnt < polTMaxJobCnt ) {
					// 추가 수행이 가능한 정책만 등록
					XLMemInfo.HT_JOBQ_POL_CNT_TMP.put(polName, polRJobCnt);						
				}					
				
			} // for-end
			
			*/
			
			// 해당 정책
			
			// ayzn - XLInit 기능 개발 : runJobQ -> runPol로 변경 및 에러처리 변경
			/*
			 *
			// 4. 3번의 HT_JOBQ_POL_CNT_TMP에 등록된 대상 정책들에 대한 조건체크후 JOB 수행
			if ( !runJobQ(cataConn) ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run JobQ ... Retry next Job polling interval time");
				Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
				continue;

			}
			*
			*/
			
			// ayzn - XLInit 기능 개발 - 4.정책대상 수행
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
			
			// ayzn - XLInit 기능 개발  / initThread추가 : JOBQ 관련 주석
			/*
 
			pstmt_updateStatus = mDBMgr.getPstmtUpdateJobQStatus(_cataConn);
			if ( pstmt_updateStatus == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to make updateJobQStatus PreparedStatement");
				return false;
			}
			
			Vector vt = mDBMgr.getJobToRun(_cataConn);
			if ( vt == null ) {
				return false;
			}
			
			if ( vt_info == null ) {
				return false;
			}
			
			*/
			
			
			// ayzn - XLInit 기능 개발  / initThread추가 : source db정보로  필요한 정보 세팅
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
