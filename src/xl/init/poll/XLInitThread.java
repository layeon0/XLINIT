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
public class XLInitThread extends Thread {
	
	// ayzn - XLInit 기능 개발  - InitThread : 받아오는 옵션 값 세팅(정책명, 그룹명, 테이블명)
	private String polCode="";
	private String grpCode="";
	private String tableName="";

	public XLInitThread()
	{
	}
	
	// ayzn - XLInit 기능 개발  - InitThread : 생성자 추가
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
		
		// ayzn - XLInit 기능 개발  - InitThread : 기존  polling 방식의 while 주석처리
		//while ( true ) {
		if ( XLInit.STOP_FLAG ) {
			XLLogger.outputInfoLog(" init thread thread is stopped - stop request");
			return;
		}
		
		// 메모리 초기화
		XLMemInfo.HT_JOBQ_DBMS_TMP.clear();
		
		// ayzn - XLInit 기능 개발  - InitThread : CNT관련 메모리 주석
		// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
		/*XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.clear();
		
		XLMemInfo.HT_JOBQ_POL_CNT_TMP.clear();*/

		// 여기서는 여러 Query를 수행해야 하므로, 주기마다 Connection 을 생성하고 재사용한후 , close 한다. 
		cataConn = mDBMgr.createConnection(false);
		Vector vt = null;
		try {
			
			// ayzn - XLInit 기능 개발  - InitThread : IDL의 JOBQ 테이블처리 주석
			///// Running 중인 JOB 들중 작업 강제종료 시간이 지난 작업들은 여기서 ABORT
			////// W 작업중 ETIME 날자가 초과한 작업들은 CANCEL
			/*vt = mDBMgr.getJobToCancelOrAbort(cataConn);
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get getJobToCancelOrAbort information.. Retry next Job polling interval time");
				
			} else {
				if ( vt.size() > 1) {
					cancelOrAbortJob(cataConn, vt);
				}
				
			}
		
			 */
					
			// ayzn - XLInit 기능 개발  - InitThread : 1. 소스 DB 정보 세팅
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
			
			// ayzn - XLInit 기능 개발  - InitThread : JOBQ -> SOURCE DB 정보 추출
			// 1. JobQ에서  소스 DBMS 별로 Running 중인 JOB의 정보 추출
			//vt = mDBMgr.getRJobCntByDbms(cataConn);
			Vector vt_info = mDBMgr.getSourceInfo(cataConn, this.grpCode, this.polCode, dicOwner, dicTname);
							
			// ayzn - XLInit 기능 개발  - InitThread :  size < 1 체크 추가
			//if ( vt_info == null) {
			if ( vt_info == null || vt_info.size() < 1){
				XLLogger.outputInfoLog("[WARN] Failed to get information.. ");
				vt_info = new Vector(); // JOBQ에 데이터가 하나도 없는 것처럼 처리하기 위해.
				System.exit(0);
			}
		
			XLLogger.outputInfoLog(vt_info);
			 
			///// TODO Running 중인 JOB 들중 작업 강제종료 시간이 지난 작업들은 여기서 
			///// 강제 종료시기코 continue;
					
					
			// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
			/**
			Hashtable<String, Integer> htRCntDBMS = new Hashtable<String, Integer>();
			for ( int i=0; i<vt.size(); i++ ) {
				Hashtable ht = (Hashtable)vt.get(i);
				String sdbIp_tmp = (String)ht.get("JOBQ_SDBIP");
				String sdbSid_tmp = (String)ht.get("JOBQ_SDBSID");
				String dbmsKey_tmp = sdbIp_tmp + "_" + sdbSid_tmp;
				
				htRCntDBMS.put(dbmsKey_tmp, Integer.parseInt((String)ht.get("RCNT")) );
			}
			**/
			
			// ayzn - XLInit 기능 개발  - InitThread : JOBQ -> SOURCE DB 정보 추출 및 HT_JOBQ_DBMS_RCNT_TMP 주석
			
			// 2. dbms_sid별  Running 중인 job이 maxJobCnt를 초과하지 않는 것들만 1차 수행대상
			//    - 이때, Running중이 지 않은 job이 해당 소스 DBMS에서 수행되는 것이 것에 대한 cnt도 추출(0)
			for ( int i=0; i<vt_info.size(); i++ ) {
					
				Hashtable ht = (Hashtable)vt_info.get(i);
				
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
				
				// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
				//int rCnt = Integer.parseInt((String)ht.get("RCNT"));
				
				String dbmsKey = sdbIp + "_" + sdbSid;
				XLLogger.outputInfoLog("[dbmsKey]"+dbmsKey);
				
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
				//if  ( rCnt >= dbmsInfo.getMaxJobCnt() ) {
					
				//	XLLogger.outputInfoLog("[JOBQ] Already Running max concurrent job - " + sdbIp + "/" + sdbSid);
				//	XLLogger.outputInfoLog("       Running Job : " + rCnt + " >= Concurrent Max Job : " + dbmsInfo.getMaxJobCnt()); 
					
				//} else {
					
					// JOBQ 수행 대상 DBMS 정보 등록
					XLMemInfo.HT_JOBQ_DBMS_TMP.put(sdbIp, sdbSid);
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					//XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.put(sdbIp + "_" + sdbSid, rCnt);
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					//rCnt++;
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					//htRCntDBMS.put(dbmsKey, rCnt);
				//}
					
			} // for-end
			
			
			// 3.HT_JOBQ_DBMS중 수행가능한 정책 정보 메모링 등록
			//   - 정책별 Runnung 중인 작업수(count : runPolJobCnt 추출해서 HTRUNNING_POL_CNT 에 등록
			//   - runPolJobCnt < tmaxJobCnt 인 것들만 작업수행 대상임
			// ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는  코드 주석
			/*vt = mDBMgr.getRJobCntByPol(cataConn);
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
				
			} // for-end*/
			
			
			
			// 해당 정책
			
			// 4. 3번의 HT_JOBQ_POL_CNT_TMP에 등록된 대상 정책들에 대한 조건체크후 JOB 수행
			// ayzn - XLInit 기능 개발  - InitThread : 함수명 runPol로 변경 및 continue에서 시스템종료로 변경
			//if ( !runJobQ(cataConn) ) {
			if ( !runPol(cataConn,vt_info) ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Pol ...");
				Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
				System.exit(0);

			}	
					
			XLLogger.outputInfoLog("");

			XLInit.POLLING_EVENTQ.waitEvent();
			
			
			
			// Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
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
			
			// 수행가능한 정책들에 대해(HT_JOBQ_POL_CNT) 세부 수행조건 체크하면서 수행
			// 세부 수행조건
			// 정책별 동시 최대작업수 < 정책별 현재 수행중인 작업 인것들 만 수행 
			XLMDBManager mDBMgr = new XLMDBManager();
			// ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는  JOPQ 관련 코드 주석
			/*pstmt_updateStatus = mDBMgr.getPstmtUpdateJobQStatus(_cataConn);
			if ( pstmt_updateStatus == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to make updateJobQStatus PreparedStatement");
				return false;
			}
			
			Vector vt = mDBMgr.getJobToRun(_cataConn);
			if ( vt == null ) {
				return false;
			}*/
			
			
			// ayzn - XLInit 기능 개발  - InitThread : source db정보로  필요한 정보 세팅
			for (int i=0; i<vt_info.size(); i++) {
				
				Hashtable ht = (Hashtable)vt_info.get(i);
				
				/*String polName = (String)ht.get("JOBQ_POLNAME");				
				if (  !XLMemInfo.HT_JOBQ_POL_CNT_TMP.containsKey(polName) ) {
					// maxJobCnt가 초과되어 삭제되었거나, 수행대상이 아닌 정책 skip
					continue;
				}*/
				
				// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
				String dbmsKey = sdbIp + "_" + sdbSid;
				
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				//int rCnt_DBMS = XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.get(dbmsKey);
				
				//XLLogger.outputInfoLog("DEBUG rCnt_DBMS/MAX_DBMS_JOB = " + rCnt_DBMS + "/" + dbmsInfo.getMaxJobCnt());
				
				//if (  rCnt_DBMS >=  dbmsInfo.getMaxJobCnt() ) {
					// dbms 별 maxJobCnt가 초과되어 삭제되었거나, 수행대상이 아닌 정책 skip
					//continue;
				//}
				
				// 정책 수행
				// 1. 정책 수행 정보 생성 JobRunPolInfo
				// ayzn - XLInit 기능 개발  - InitThread : condWhere제외 주석처리
				//long jobSeq = Long.parseLong((String)ht.get("JOBQ_SEQ"));
				String condWhere = (String)ht.get("TB_CONDITION");
				//int tmaxCnt = Integer.parseInt((String)ht.get("POL_TMAX_JOBCNT"));
				
				XLLogger.outputInfoLog("  condWhere : ["+condWhere +"]");
				XLLogger.outputInfoLog("");
				
				// ayzn - XLInit 기능 개발  - InitThread : 인자값 수정
				//XLJobRunPol jobRunPolInfo = new XLJobRunPol(jobSeq, polName, condWhere);
				XLJobRunPol jobRunPolInfo = new XLJobRunPol(grpCode, polCode, tableName, condWhere);
				if ( !jobRunPolInfo.makeInfo(_cataConn) ) {
					XLLogger.outputInfoLog("[WARN] Failed to Run Job policy - " + polCode);
					return false;
				}
				
				// ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는  cnt메모리 관련 코드 주석
				// 해당 정책의 running count 증가 (-1일 감소)
				/*XLMemInfo.plusRJobCnt(polName, 1); 
				
				if ( XLMemInfo.getRJobCnt(polName) >= tmaxCnt ) {
					// 이후에는 수행하면 안되므로, 이 정책 정보는 삭제
					// 삭제되었으므로, 이 정책의 JOB은  skip됨.
					XLMemInfo.removeRJobCntInfo(polName);
					
					// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
					// !!!!! continue???
					//continue;
					
				}*/
				
				// ayzn - XLInit 기능 개발  - InitThread : jobseq제외하고 처리
				// 해당 정책의 running job 메모리에 등록
				//XLMemInfo.addRJobPolInfo(polName, jobSeq, jobRunPolInfo);
				XLMemInfo.addRJobPolInfo(polCode, jobRunPolInfo);
				
				// ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는 코드 주석
				// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
				// DBMS rCNT 증가
				//XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.put(dbmsKey, ++rCnt_DBMS);
				
				jobRunPolInfo.setsDate(XLUtil.getCurrentDateStr());
				
				// ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는 코드 주석
				/*
				// JOB STATUS update W-->R
				pstmt_updateStatus.setString(1, "R");
				
				// pstmt_updateStatus.setString(2, jobRunPolInfo.getsDate());
				// setTimestamp(_setCnt, Timestamp.valueOf(_value));
				// cksohn - catalog ppas
				// cksohn - SCHED_ETIME 체크 오류 수정
				
				if ( jobRunPolInfo.getsDate() == null || jobRunPolInfo.getsDate().equals("") ) {
					pstmt_updateStatus.setNull(2, java.sql.Types.NULL);
				} else {
					pstmt_updateStatus.setTimestamp(2, Timestamp.valueOf(jobRunPolInfo.getsDate()) );	
				}
				
				pstmt_updateStatus.setLong(3, jobSeq);
				pstmt_updateStatus.setString(4, polName);
				pstmt_updateStatus.executeUpdate();
				_cataConn.commit(); // 이건 건단위로 commit
				*/
				
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
	
	
	// Cancel or Abort
	/*private void cancelOrAbortJob(Connection _cataConn, Vector _vtJob)
	{
				
		try {
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			XLLogger.outputInfoLog("#####  CALCEL OR ABORT JOB PROCESSING #####");
			
			
			for ( int i=0; i<_vtJob.size(); i++) {
				
				Hashtable ht = (Hashtable)_vtJob.get(i);
				
				long jobSeq = Long.parseLong((String)ht.get("JOBQ_SEQ"));
				String polName = (String)ht.get("JOBQ_POLNAME");
				String condWhere = (String)ht.get("JOBQ_CONDITION_WHERE");
				
				String jobQ_status = (String)ht.get("JOBQ_STATUS");
				
				// JobQ Jobq 세부 정보 생성
				XLJobRunPol jobRunPol = new XLJobRunPol(jobSeq, polName, condWhere);
				if ( !jobRunPol.makeInfo(_cataConn) ) {
					XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to make JobQ information - " + polName);
					continue;
				}
				
				// if ( jobQ_status.equals(XLCons.STATUS_CANCEL) ) {
				// cksohn - SCHED_ETIME 체크 오류 수정
				if ( jobQ_status.equals(XLOGCons.STATUS_WAIT) ) {
					
				
					jobRunPol.setJobStatus(XLOGCons.STATUS_CANCEL);
					jobRunPol.setErrMsg_Apply("CANCEL Waiting job because of Job start end time over.");
					
					// 2. status 에 따른 정보갱신 및 REPORT 결과 저장
					//  2-1 REPORT 테이블 결과저장
					if ( !mDBMgr.insertJobResultReport(_cataConn, jobRunPol, jobRunPol.getCondCommitCnt()) ) {
						XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to insert job result report - " + jobRunPol.getCondWhere());
					}
					
					//  2-2 CONDITION 테이블 STATUS update
					if ( !mDBMgr.updateJobResultCond(_cataConn, jobRunPol) ) {
						XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to update job result condition_action - " + jobRunPol.getCondWhere());
					}
								
					//  2-3 JOBQ 테이블 삭제
					if ( !mDBMgr.deleteJobQ(_cataConn, jobRunPol) ) {
						XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to delete jobQ - " + jobRunPol.getCondWhere());
					}
					
					XLLogger.outputInfoLog("[CANCEL JOB][cancelOrAbortJob] " + jobSeq + " / " + polName);
					
				} else { // ABORT JOB Processing
					
					
					String key = polName + "_" + jobSeq;
					if ( XLMemInfo.HT_RUNNING_JOB_INFO.containsKey(key) ) {
						XLLogger.outputInfoLog("[ABORT JOB][cancelOrAbortJob] " + jobSeq + " / " + polName + " Request. - Abort next polling time");
						
						try { 
							XLJobRunPol jobPolInfo = XLMemInfo.HT_RUNNING_JOB_INFO.get(key);
							jobPolInfo.setErrMsg_Recv("ABORT Running job because of Job execution end time over.");
							jobPolInfo.setStopJobFlag(true);
							
						} catch (Exception ee) {
							XLException.outputExceptionLog(ee);
							
						}
						
					} else {
						XLLogger.outputInfoLog("[ABORT JOB][cancelOrAbortJob] Cannot find Running Job Info in Memory - " + jobSeq + " / " + polName );
					}
					
				}
				
			} // for-end
			
			_cataConn.commit();

			XLLogger.outputInfoLog("###########################################");

			 
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
		} finally {

		}
		
	}*/
}
