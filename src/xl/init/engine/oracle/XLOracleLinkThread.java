package xl.init.engine.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Vector;

import oracle.jdbc.OraclePreparedStatement;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.init.conn.XLOracleConnection;
import xl.init.dbmgr.XLDBCons;
import xl.init.dbmgr.XLMDBManager;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLInit;
import xl.init.main.XLOGCons;
import xl.init.util.XLException;

// gssg - SK실트론 O2O
// gssg - linkMode 지원
public class XLOracleLinkThread extends Thread {

	private XLJobRunPol jobRunPol = null;
	

	private Connection cataConn = null;
	private PreparedStatement pstmtUpdateJobQCommitCnt = null;
	private PreparedStatement pstmtUpdateCondCommitCnt = null;

	Connection connection = null;
	private OraclePreparedStatement insertSelectStmt = null;
	private Statement st = null;

	private long applyCnt = 100;
	private long totalCommitCnt =0;

	
	long totalApplyCnt = 0;
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";

	
	public XLOracleLinkThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;		
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][LINK]";

	}

	public void run() {
				
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting LinkThread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);	
			this.pstmtUpdateJobQCommitCnt = mDBMgr.getPstmtUpdateJobQCommitCnt(this.cataConn);
			this.pstmtUpdateCondCommitCnt = mDBMgr.getPstmtUpdateCondCommitCnt(this.cataConn);

			
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();
						

	        String url = "jdbc:oracle:thin:@//" + sdbInfo.getIp() + ":" + sdbInfo.getPort() + "/" + sdbInfo.getDbSid();
	        
	        try {
	            Class.forName("oracle.jdbc.driver.OracleDriver");
	        } catch (ClassNotFoundException e) {
				XLLogger.outputInfoLog(this.logHead +  "[EXCEPTION] Oracle JDBC driver not found.");
				errMsg = "[EXCEPTION] Failed to make source db connection - " + sdbInfo.getIp() + "/" + sdbInfo.getDbSid();
				XLLogger.outputInfoLog(errMsg);
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead +  "[EXCEPTION] Link Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Recv(errMsg);

	            e.printStackTrace();
	            return;
	        }
	        
	        
	        for ( int i = 0; i < XLConf.XL_DBCON_RETRYCNT; i++ ) {	
	        	try {
		        	this.connection = DriverManager.getConnection(url, sdbInfo.getUserId(), sdbInfo.getPasswd());

					XLLogger.outputInfoLog(this.logHead + " Source DBMS is connected - " +  sdbInfo.getIp() + "/" + sdbInfo.getDbSid());

					stime = System.currentTimeMillis();
					
					this.connection.setAutoCommit(false);
					
					st = this.connection.createStatement();			
					st.executeQuery("alter session set nls_date_format='" + XLDBCons.DATE_FORMAT + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					
					st = this.connection.createStatement();		
					st.executeQuery("alter session set nls_timestamp_format='" + XLDBCons.TIMESTAMP_FORMAT + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					
					st = this.connection.createStatement();		
					st.executeQuery("alter session set nls_timestamp_tz_format='" + XLDBCons.TIMESTAMP_TZ_FORMAT + "'");
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
					
					break;
					
				} catch (Exception e) {

					e.printStackTrace();
					
					// gssg - Troubleshooting 에러 코드 생성
					XLLogger.outputInfoLog("[E1002] DB Connection by manager failed!("+ (i+1) +") " + sdbInfo.getIp() + sdbInfo.getDbSid());
					try { if(this.connection != null) this.connection.close(); } catch(Exception e1) {} finally { this.connection = null; }
					try{
						Thread.sleep(10000);				
					}catch(Exception se){}
				
				} finally {
					try{ if ( st != null ) st.close(); } catch (Exception e) {} finally { st = null; }
				}
	        }
				
            this.insertSelectStmt = (OraclePreparedStatement)connection.prepareStatement(this.jobRunPol.getTarInsertSql() +  this.jobRunPol.getSrcSelectSql());

            // 쿼리 실행
            applyCnt = insertSelectStmt.executeUpdate();
    		etime = System.currentTimeMillis();			
							
				if ( XLInit.STOP_FLAG ) {
					
					
					errMsg = "[STOP] LinkThread is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					this.connection.rollback();
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					
					this.jobRunPol.setStopJobFlag(true);
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
					FinishJob();
					
					return;
				
			}
				
				if ( this.jobRunPol.isStopJobFlag() ) {
					// ABORT JOB
					errMsg = "[ABORT] LinkThread is stopped by Job ABORT request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					this.connection.rollback();
					
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					
					this.jobRunPol.setStopJobFlag(true);
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
					
					FinishJob();
					
					return;
					
					
				}	

			long elapsedTime = (etime-stime) / 1000;

			XLLogger.outputInfoLog(this.logHead + " Completed Job Recv : " + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("\tTotal InsertSelect count : " + applyCnt + " / Elapsed time(sec) : " +  elapsedTime);


			this.connection.commit();

			this.totalCommitCnt += this.applyCnt;
			this.totalApplyCnt += this.applyCnt;
			
			this.applyCnt = 0;
			
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
			
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_SUCCESS);
			FinishJob();
			
			return;

		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {
			
			try { if ( this.insertSelectStmt != null ) this.insertSelectStmt.close(); } catch (Exception e) {} finally { this.insertSelectStmt = null; }			
			try { if ( this.connection != null ) this.connection.close(); } catch (Exception e) {} finally { this.connection = null; }
			
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
			//try { if ( this.pstmtUpdateJobQCommitCnt != null ) this.pstmtUpdateJobQCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateJobQCommitCnt = null; }
			//try { if ( this.pstmtUpdateCondCommitCnt != null ) this.pstmtUpdateCondCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateCondCommitCnt = null; }
			try { if ( this.cataConn != null ) this.cataConn.close(); } catch (Exception e) {} finally { this.cataConn = null; }			

		}
		
		

		
	}

		private void FinishJob()
		{
			try {
				
				// XLLogger.outputInfoLog("[FINISH JOB] START - " + this.jobRunPol.getPolName());
				// cksohn - xl - 수행결과 status log 에 로깅하도록
				XLLogger.outputInfoLog("[FINISH JOB][" +  this.jobRunPol.getPolName() + "] totalCommitCnt : " + this.totalCommitCnt);
				
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
