package xl.init.engine.postgresql;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.Vector;

import oracle.jdbc.OraclePreparedStatement;
import xl.init.conf.XLConf;
import xl.init.engine.oracle.XLOracleLoaderThread;
import xl.init.logger.XLLogger;
import xl.init.conn.XLMySQLConnection;
import xl.init.conn.XLOracleConnection;
import xl.init.conn.XLPPASConnection;
import xl.init.conn.XLPostgreSQLConnection;
import xl.init.dbmgr.XLMDBManager;
import xl.init.engine.postgresql.XLPostgreSQLLoaderThread;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLMemInfo;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;



/**
 * 
 * @author gssg
 * 
 * cksohn - BULK mode oracle sqlldr
 *
 */

// gssg - xl PPAS/PostgreSQL 분리

public class XLPostgreSQLApplyBulkThread extends Thread {
	
	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
	// gssg - t2p 보완
	// gssg - postgreSQL 커넥터 수정
	private XLPostgreSQLConnection postgreSQLConnObj = null;
	private PreparedStatement pstmtInsert = null;	
	
	
	private Connection cataConn = null;
	// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
	//private PreparedStatement pstmtUpdateJobQCommitCnt = null;
	//private PreparedStatement pstmtUpdateCondCommitCnt = null;

	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // 기존 수행에서 commit된 건수(condCommitCnt) + 이번 수행에서 commit된 건수(applyCnt)
	
	long totalApplyCnt = 0; // 이번 작업수행시 실제로 insert를 수행한 건수 (누적건수 아님)
	
	// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
	private long failedCnt = 0;

	// gssg - xl o2p bulk mode 지원
	// gssg - mysql -> ppas 변경
	private XLPostgreSQLLoaderThread loaderThread = null;
		
	public boolean isWait = false;
		
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLPostgreSQLApplyBulkThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		this.totalCommitCnt = this.jobRunPol.getCondCommitCnt();
				
		// 대상 테이블의 컬럼 정보
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][APPLY BULK]";
	}


	@Override
	public void run(){
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		// gssg - xl m2m bulk mode 지원
		loaderThread = new XLPostgreSQLLoaderThread(this.jobRunPol);
		loaderThread.start();

		
		// gssg - xl m2m bulk mode 지원
		// synchronized(loaderThread) {

		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting ApplyThread(Direct Path Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);			
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
			//this.pstmtUpdateJobQCommitCnt = mDBMgr.getPstmtUpdateJobQCommitCnt(this.cataConn);
			//this.pstmtUpdateCondCommitCnt = mDBMgr.getPstmtUpdateCondCommitCnt(this.cataConn);
			 
			
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
				
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			int MAX_CHECK_CNT = 10;
			int chkCnt = 0;
						
			// gssg - xl m2m bulk mode 지원
//			 XLManager.POLLING_EVENTQ.waitForeverEvent();
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- START WHILE!!!!! - " + this.jobRunPol.isRunLoader());
			}
									
            // gssg - xl 전체적으로 보완
            // gssg - o2p bulk mode thread 순서 조정
			// gssg - o2p bulk mode 스레드 순서 조정
			while ( !this.jobRunPol.isRunLoader() && chkCnt <= MAX_CHECK_CNT ) {
//			while ( !this.jobRunPol.isLoadQuery() && chkCnt <= MAX_CHECK_CNT ) {
								
				chkCnt++;
				XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][APPLY BULK][LOADER] Waiting Run Loader.(" + chkCnt + ")");
				Thread.sleep(1000);
			}
						
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- END WHILE!!!! - " + this.jobRunPol.isRunLoader());
			}

			// gssg - xl m2m bulk mode 지원
//			XLInit.POLLING_EVENTQ.notifyEvent();
			
			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
										
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
					// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
					// this.oraConnObj.rollback();
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
										
					this.jobRunPol.setStopJobFlag(true);
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
					FinishJob();
					
					return;
				}
				
//				if ( XLConf.XL_MGR_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] jobRunPol.isStopJobFlag = " + jobRunPol.isStopJobFlag() );
//				}
				
				if ( this.jobRunPol.isStopJobFlag() ) {
					// ABORT JOB
					errMsg = "[ABORT] Apply is stopped by Job ABORT request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
					// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
					// this.oraConnObj.rollback();
					
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
										
					this.jobRunPol.setStopJobFlag(true);
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
					
					FinishJob();
					
					return;
					
					
				}
					
				if( dataQ.size() == 0 )
				{
					// Recv Thread의 상태는 Recv Thread의 비정상 종료시 메시지를 setting 하도록 되어 있는데, 
					// 이를 가지고 check 하자. 
					if ( this.jobRunPol.getErrMsg_Recv() != null ) {
						// RECV Thread 비정상 종료시
						
						errMsg = "Recv Thread is stopped abnormal : " + this.jobRunPol.getCondWhere();
						XLLogger.outputInfoLog(this.logHead + errMsg);
						
						// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
						// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
						// this.oraConnObj.rollback();
											
						this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
						
						
						this.jobRunPol.setStopJobFlag(true);
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
						FinishJob();
						
						return;
						
					}
					
					// cksohn - BULK mode oracle sqlldr
					if ( this.jobRunPol.getErrMsg_Loader() != null ) {
						// Loader Thread 비정상 종료시
						
						errMsg = "Loader Thread is stopped abnormal : " + this.jobRunPol.getCondWhere();
						XLLogger.outputInfoLog(this.logHead + errMsg);
						
						// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
						// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
						// this.oraConnObj.rollback();;
											
						this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
						
						
						this.jobRunPol.setStopJobFlag(true);
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
						FinishJob();
						
						return;
					}

					
					//XLLogger.outputInfoLog("CKSOHN DEBUG----- this.jobRunPol.isAliveRecvBulkThread() " + this.jobRunPol.isAliveRecvBulkThread());
					//XLLogger.outputInfoLog("CKSOHN DEBUG----- this.loaderThread.isAlive() " + this.loaderThread.isAlive());
					
					
					// 모든 작업의 정상 종료여부 check
					
					//boolean loaderAlive = true;
					//try { loaderAlive = this.loaderThread.isAlive(); } catch (Exception ee) { loaderAlive = false; }
					// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
					boolean loaderAlive = this.jobRunPol.isRunLoader();
					if ( XLConf.XL_DEBUG_YN ) {
						XLLogger.outputInfoLog("[DEBUG] loaderAlive == " + loaderAlive);
					}
					
					if ( !this.jobRunPol.isAliveRecvBulkThread() &&  !loaderAlive) {
						
						// ErrMsg도 없는 상태에서 Recv와 Loader Thread가 종료되었다면, 일단 정상 종료로 처리
						XLLogger.outputInfoLog(this.logHead + " All Job Thread is finished.");
						
						
						// 누적 commt 건수
						// this.totalCommitCnt += this.applyCnt;
						// this.totalApplyCnt += this.applyCnt;
						// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
						// gssg - xl m2m bulk mode 지원
						// String bulkLoadedCnt = getBulkLoadedCnt();
						// String bulkLoadedCnt = "10:0";
						// gssg - xl m2m bulk mode logging 지원
						long bulkLoadedCnt = jobRunPol.getApplyCnt(); // gssg - xl recvCnt를 applyCnt로 적용
						
						// if ( bulkLoadedCnt != null ) {
							// StringTokenizer st = new StringTokenizer(bulkLoadedCnt, ":");
							
							// this.totalCommitCnt  = Long.parseLong(st.nextToken());
							this.totalCommitCnt  = bulkLoadedCnt;
							this.totalApplyCnt   = this.totalCommitCnt;
							// this.failedCnt = Long.parseLong(st.nextToken());
							this.failedCnt = 0;
						// }
						
						// gssg - xl m2m bulk mode logging 지원
						// gssg - 불필요 기능 주석 처리
						// XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt + " / " + this.jobRunPol.getPolName());
						XLLogger.outputInfoLog(this.logHead + " Policy Name : " + this.jobRunPol.getPolName());
						this.applyCnt = 0; // 초기화
						// XLLogger.outputInfoLog("CKSOHN DEBUG this.totalCommitCnt FINAL = " + this.totalCommitCnt);
						
						// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
						/*this.pstmtUpdateJobQCommitCnt.setLong(1,  this.totalCommitCnt);
						this.pstmtUpdateJobQCommitCnt.setLong(2,  this.jobRunPol.getJobseq());
						this.pstmtUpdateJobQCommitCnt.setString(3,  this.jobRunPol.getPolName());
						this.pstmtUpdateJobQCommitCnt.executeUpdate();
						
						this.pstmtUpdateCondCommitCnt.setLong(1,  this.totalCommitCnt);
						this.pstmtUpdateCondCommitCnt.setString(2,  this.jobRunPol.getPolName());
						this.pstmtUpdateCondCommitCnt.setLong(3,  this.jobRunPol.getCondSeq());
						// gssg - 일본 네트워크 분산 처리
						this.pstmtUpdateCondCommitCnt.setLong(4, this.jobRunPol.getWorkPlanSeq());

						this.pstmtUpdateCondCommitCnt.executeUpdate();
						this.cataConn.commit();*/
						
						if ( XLConf.XL_DEBUG_YN ) {
							XLLogger.outputInfoLog("[DEBUG] commitCnt-FINAL : " + this.totalCommitCnt);
						}
						
						etime = System.currentTimeMillis();
						
						long elapsedTime = (etime-stime) / 1000;
						
						//XLLogger.outputInfoLog("");
						XLLogger.outputInfoLog(this.logHead + " Completed Job Apply : " +  this.jobRunPol.getCondWhere());
						XLLogger.outputInfoLog("\tTotal Insert count : " + this.totalApplyCnt + " / Elapsed time(sec) : " +  elapsedTime);
						//XLLogger.outputInfoLog("");
						
						
					
						// TODO : JOBQ & RESULT UPDATE Success
						// this.jobRunPol.setJobStatus(XLCons.STATUS_SUCCESS);
						// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
						// gssg - xl m2m bulk mode logging 지원
//						if ( bulkLoadedCnt == null || (this.failedCnt != 0 ) ) { // gssg - 불필요 기능 주석 처리
//							this.jobRunPol.setJobStatus(XLCons.STATUS_FAIL);
//						}
						if (this.failedCnt != 0) {
							this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
						}else {
							this.jobRunPol.setJobStatus(XLOGCons.STATUS_SUCCESS);
						}
						
						FinishJob();
						
						break;
						
					}
					
					isWait = true;
					dataQ.waitDataQ();
					continue;
				}

			} // while-end
			
			
			
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead +"[EXCEPTION] " +  errMsg);
			
			// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
			// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
			// this.oraConnObj.rollback();
			
			// 뒷처리
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			
			FinishJob();
			
		} finally {
			
			// gssg - xl m2m bulk mode 지원
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			// gssg - xl o2p bulk mode 지원
			// gssg - mysql -> ppas 변경
			try { if ( this.postgreSQLConnObj != null ) this.postgreSQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.postgreSQLConnObj = null; }
			
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
			//try { if ( this.pstmtUpdateJobQCommitCnt != null ) this.pstmtUpdateJobQCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateJobQCommitCnt = null; }
			//try { if ( this.pstmtUpdateCondCommitCnt != null ) this.pstmtUpdateCondCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateCondCommitCnt = null; }
	
			try { if ( this.cataConn != null ) this.cataConn.close(); } catch (Exception e) {} finally { this.cataConn = null; }

		
			
			if ( XLConf.XL_DEBUG_YN ) {
				// gssg - xl m2m bulk mode 지원
				// XLLogger.outputInfoLog("[DEBUG] this.jobRunPol.isAliveRecvThread() = " + this.jobRunPol.isAliveRecvThread());
				XLLogger.outputInfoLog("[DEBUG] this.jobRunPol.isAliveRecvThread() = " + this.jobRunPol.isAliveRecvBulkThread());
				
			}
			// 1. Recv Thread check & interrupt -여기서 해 줘야 하나 ?!?!?!?
			//if ( this.jobRunPol.isAliveRecvThread() ) {
				// XLLogger.outputInfoLog(this.logHead + "[FINISH JOB] RecvThread is still alive. stop RecvThread");
			
				// this.jobRunPol.stopRecvThread();
				this.jobRunPol.stopRecvBulkThread();
				
				// cksohn - BULK mode oracle sqlldr
				try { if ( this.loaderThread != null ) this.loaderThread.interrupt(); } catch (Exception ee) {} finally { this.loaderThread = null; }
				
			//}
			
			// 메모리 정리는 여기서!!!!!
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobseq 제외
			//XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName(), this.jobRunPol.getJobseq());
			XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName());
			
			
			XLInit.POLLING_EVENTQ.notifyEvent();
			}
		// }
		
	}
	
	
	
	// JOB 종료
	private void FinishJob()
	{
		try {
			
			// XLLogger.outputInfoLog("[FINISH JOB] START - " + this.jobRunPol.getPolName());
			// cksohn - xl - 수행결과 status log 에 로깅하도록
			XLLogger.outputInfoLog("[FINISH JOB][" + this.jobRunPol.getPolName() + "] totalCommitCnt : " + this.totalCommitCnt);
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			// 1. Recv Thread check & interrupt -여기서는 이거하면 안된다.
			//if ( this.jobRunPol.isAliveRecvThread() ) {
			//	XLLogger.outputInfoLog("[FINISH JOB] RecvThread is still alive. stop RecvThread");
			//	this.jobRunPol.stopRecvThread();
			// }
			
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : report, condition, jobq 테이블 관련 처리 주석
			// 2. status 에 따른 정보갱신 및 REPORT 결과 저장
			//  2-1 REPORT 테이블 결과저장
			/*if ( !mDBMgr.insertJobResultReport(this.cataConn, this.jobRunPol, this.totalCommitCnt) ) {
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Failed to insert job result report - " + this.jobRunPol.getCondWhere());
			}
			
			//  2-2 CONDITION 테이블 STATUS update
			if ( !mDBMgr.updateJobResultCond(this.cataConn, this.jobRunPol) ) {
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Failed to update job result condition_action - " + this.jobRunPol.getCondWhere());
			}
						
			//  2-3 JOBQ 테이블 삭제
			if ( !mDBMgr.deleteJobQ(this.cataConn, this.jobRunPol) ) {
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Failed to delete jobQ - " + this.jobRunPol.getCondWhere());
			}
			
			
			this.cataConn.commit();*/
			
			 
			// XLLogger.outputInfoLog("[FINISH JOB] END - " + this.jobRunPol.getPolName());
			// cksohn - xl - 수행결과 status log 에 로깅하도록 start - [
			String resultStatus = "SUCCESS";
			if ( this.jobRunPol.getJobStatus().equals(XLOGCons.STATUS_FAIL) ) {
				resultStatus = "FAIL";
			} else if ( this.jobRunPol.getJobStatus().equals(XLOGCons.STATUS_ABORT) ) {
				resultStatus = "ABORT";
			}
			// XLLogger.outputInfoLog("[FINISH JOB] END - " + this.jobRunPol.getPolName() + " - " + resultStatus);
			// cksohn - xl - 수행결과 status log 에 로깅하도록
			XLLogger.outputInfoLog("[FINISH JOB][" + this.jobRunPol.getPolName() + "] RESULT - " + resultStatus);
			
			// ] - end cksohn - xl - 수행결과 status log 에 로깅하도록
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
		}
		
	}	
	
}
