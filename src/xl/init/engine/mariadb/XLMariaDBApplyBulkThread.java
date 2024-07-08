package xl.init.engine.mariadb;

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
import xl.init.conn.XLMariaDBConnection;
import xl.init.conn.XLOracleConnection;
import xl.init.dbmgr.XLMDBManager;
import xl.init.engine.mariadb.XLMariaDBLoaderThread;
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
 * gssg - BULK mode mariadb sqlldr
 *
 */

public class XLMariaDBApplyBulkThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
	
	private XLMariaDBConnection mariaDBConnObj = null;
	private PreparedStatement pstmtInsert = null;	
	
	
	private Connection cataConn = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // 기존 수행에서 commit된 건수(condCommitCnt) + 이번 수행에서 commit된 건수(applyCnt)
	
	long totalApplyCnt = 0; // 이번 작업수행시 실제로 insert를 수행한 건수 (누적건수 아님)
	
	// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
	private long failedCnt = 0;

	private XLMariaDBLoaderThread loaderThread = null;
	
	public boolean isWait = false;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLMariaDBApplyBulkThread(XLJobRunPol jobRunPol) {
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
		loaderThread = new XLMariaDBLoaderThread(this.jobRunPol);
		loaderThread.start();


		try {			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting ApplyThread(Direct Path Mode)..." + this.jobRunPol.getCondWhere());
			
			XLLogger.outputInfoLog("");
			
			
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);			
			 
			
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
				
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			int MAX_CHECK_CNT = 10;
			int chkCnt = 0;

			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- START WHILE!!!!! - " + this.jobRunPol.isRunLoader());
			}
									
			// gssg - xl 전체적으로 보완
            // gssg - m2m bulk mode thread 순서 조정
			// gssg - o2m bulk mode 스레드 순서 조정
			while ( !this.jobRunPol.isLoadQuery() && chkCnt <= MAX_CHECK_CNT ) {
								
				chkCnt++;
				XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][APPLY BULK][LOADER] Waiting Run Loader.(" + chkCnt + ")");
				Thread.sleep(1000);
			}
						
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- END WHILE!!!! - " + this.jobRunPol.isRunLoader());
			}

			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
					
					
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
					// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
					// this.oraConnObj.rollback();
					
					// gssg - xl 전체적으로 보완
					// gssg - m2m bulk mode rollback 처리
					this.mariaDBConnObj.rollback();
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					
					this.jobRunPol.setStopJobFlag(true);
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
					FinishJob();
					
					return;
				}
				
//				if ( XLConf.XL_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] jobRunPol.isStopJobFlag = " + jobRunPol.isStopJobFlag() );
//				}
				
				if ( this.jobRunPol.isStopJobFlag() ) {
					// ABORT JOB
					errMsg = "[ABORT] Apply is stopped by Job ABORT request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
					// Oracle 타겟 BULK MODE 에서는 sqlldr 를 사용하므로, Oracle 직접 connection 없음
					// this.oraConnObj.rollback();
					
					// gssg - xl 전체적으로 보완
					// gssg - m2m bulk mode rollback 처리
					this.mariaDBConnObj.rollback();
					
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
						
						// gssg - xl 전체적으로 보완
						// gssg - m2m bulk mode rollback 처리
						this.mariaDBConnObj.rollback();
											
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
						
						// gssg - xl 전체적으로 보완
						// gssg - m2m bulk mode rollback 처리
						this.mariaDBConnObj.rollback();

											
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

					// gssg - xl 전체적으로 보완
		            // gssg - m2m bulk mode thread 순서 조정
					boolean loaderAlive = this.jobRunPol.isRunLoader();
					
					if ( XLConf.XL_DEBUG_YN ) {
						XLLogger.outputInfoLog("[DEBUG] loaderAlive == " + loaderAlive);
					}
					
					if ( !this.jobRunPol.isAliveRecvBulkThread() &&  !loaderAlive ) {
						
						// ErrMsg도 없는 상태에서 Recv와 Loader Thread가 종료되었다면, 일단 정상 종료로 처리
						XLLogger.outputInfoLog(this.logHead + " All Job Thread is finished.");
						
						
						// 누적 commt 건수
						// this.totalCommitCnt += this.applyCnt;
						// this.totalApplyCnt += this.applyCnt;
						// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
						// gssg - xl m2m bulk mode 지원
						// String bulkLoadedCnt = getBulkLoadedCnt();
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
			try { if ( this.mariaDBConnObj != null ) this.mariaDBConnObj.closeConnection(); } catch (Exception e) {} finally { this.mariaDBConnObj = null; }
				
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
			XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName());
			
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
	
	
	// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
	// return String 
	//			successCnt:FailedTotalCnt 
	//          -- log파일이 존재하지 않건, Exception 발생시 null
	
	private String getBulkLoadedCnt()
	{
		
		
		if ( XLConf.XL_DEBUG_YN ) {
			XLLogger.outputInfoLog("[DEBUG] getBulkLoadedCnt is called.....!!!");
		}
		
		BufferedReader br = null;
		
		int successCnt = 0;
		int failedCnt = 0;
		
		try {
			
			File logFile = new File(this.jobRunPol.getBulk_logFilePath());
			if ( !logFile.exists() ) {
				
				XLLogger.outputInfoLog("[WARN] Loader logfile Not Exist : " + this.jobRunPol.getBulk_logFilePath());
				return null;
				
			} else {
				// logfile 로 결과 분석
				br = new BufferedReader(new FileReader(this.jobRunPol.getBulk_logFilePath()));

				StringTokenizer st = null;
				String line = "";
				while ( (line=br.readLine()) != null ) {
					
					if ( line.contains("successfully loaded") ) {
						
						//   200000 Rows successfully loaded. loaded된 count  
						st = new StringTokenizer(line, " \t\n");
						successCnt = Integer.parseInt(st.nextToken());
						
					} else if ( line.contains("not loaded") ) { 
						//  0 Rows not loaded due to data errors.
						// 0 Rows not loaded because all WHEN clauses were failed.
						// 0 Rows not loaded because all fields were null.
						st = new StringTokenizer(line, " \t\n");
						failedCnt += Integer.parseInt(st.nextToken());
						 
					}
					
				} // while-end
				
				
				String resultCnt = successCnt + ":" + failedCnt;

				return resultCnt;
				
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		} finally {
			try { if ( br != null ) br.close(); } catch (Exception ee) {} finally { br = null; }
			
		}
	}
	
	
	// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
	// sqlldr 접속에러등 Exception 을 log로 부터 확인
	// return : null --> No error
	//          "error message"
	private String checkSqlldrError()
	{
		
		
		// XLLogger.outputInfoLog("CKSOHN DEBUG checkSqlldrError is called.....!!!");
		XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "] check sqlldr status.");
		
		BufferedReader br = null;
		
		int successCnt = 0;
		int failedCnt = 0;
		
		StringBuffer sb_error = new StringBuffer();
		
		try {
			
			File logFile = new File(this.jobRunPol.getBulk_logFilePath());
			if ( !logFile.exists() ) {
				
				sb_error.append("[WARN] Loader logfile Not Exist : " + this.jobRunPol.getBulk_logFilePath());
				XLLogger.outputInfoLog("[WARN] Loader logfile Not Exist : " + this.jobRunPol.getBulk_logFilePath());
				
				return sb_error.toString();
				
			} else {
				// logfile 로 결과 분석
				br = new BufferedReader(new FileReader(this.jobRunPol.getBulk_logFilePath()));

				StringTokenizer st = null;
				String line = "";
				while ( (line=br.readLine()) != null ) {
					
					/**
					 *  SQL*Loader-704: Internal error: ulconnect: OCIServerAttach [0]
						ORA-12514: TNS:listener does not currently know of service requested in connect descriptor
					 */
					
					if ( line.contains("Internal error") || line.contains("ORA-")) {
						
						sb_error.append(line).append("\n");
						
					} 
					
				} // while-end
				
				
				if ( sb_error.length() == 0 ) {
					return null; // NO error
				} else {
					return sb_error.toString();
				}
				
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
			// return null;
			return e.toString();
			
		} finally {
			try { if ( br != null ) br.close(); } catch (Exception ee) {} finally { br = null; }
			
		}
	}
}
