package xl.init.engine.ppas;

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
import xl.init.dbmgr.XLMDBManager;
import xl.init.engine.ppas.XLPPASLoaderThread;
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
 * @author cksohn
 * 
 * cksohn - BULK mode oracle sqlldr
 *
 */

// gssg - xl m2m bulk mode logging ����
// gssg - xl mysql ����ȭ

public class XLPPASApplyBulkThread extends Thread {
	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
	// gssg - xl o2p bulk mode ����
	// gssg - mysql -> ppas ����
	private XLPPASConnection ppasConnObj = null;
	private PreparedStatement pstmtInsert = null;	
	
	
	private Connection cataConn = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // ���� ���࿡�� commit�� �Ǽ�(condCommitCnt) + �̹� ���࿡�� commit�� �Ǽ�(applyCnt)
	
	long totalApplyCnt = 0; // �̹� �۾������ ������ insert�� ������ �Ǽ� (�����Ǽ� �ƴ�)
	
	// cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
	private long failedCnt = 0;

	// gssg - xl o2p bulk mode ����
	// gssg - mysql -> ppas ����
	private XLPPASLoaderThread loaderThread = null;
		
	public boolean isWait = false;
		
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLPPASApplyBulkThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		this.totalCommitCnt = this.jobRunPol.getCondCommitCnt();
				
		// ��� ���̺��� �÷� ����
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][APPLY BULK]";
	}


	@Override
	public void run(){
				
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
		// gssg - xl m2m bulk mode ����
		loaderThread = new XLPPASLoaderThread(this.jobRunPol);
		loaderThread.start();

		
		// gssg - xl m2m bulk mode ����
		// synchronized(loaderThread) {

		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting ApplyThread(Direct Path Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);			
			 
			
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
				
			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
			int MAX_CHECK_CNT = 10;
			int chkCnt = 0;
						
			// gssg - xl m2m bulk mode ����
//			 XLManager.POLLING_EVENTQ.waitForeverEvent();
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- START WHILE! - " + this.jobRunPol.isRunLoader());
			}
									
            // gssg - xl ��ü������ ����
            // gssg - o2p bulk mode thread ���� ����
			// gssg - o2p bulk mode ������ ���� ����
			while ( !this.jobRunPol.isRunLoader() && chkCnt <= MAX_CHECK_CNT ) {
//			while ( !this.jobRunPol.isLoadQuery() && chkCnt <= MAX_CHECK_CNT ) {
								
				chkCnt++;
				XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][APPLY BULK][LOADER] Waiting Run Loader.(" + chkCnt + ")");
				Thread.sleep(1000);
			}
						
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- END WHILE! - " + this.jobRunPol.isRunLoader());
			}
			
			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
										
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� - comment
					// Oracle Ÿ�� BULK MODE ������ sqlldr �� ����ϹǷ�, Oracle ���� connection ����
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
					
					// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� - comment
					// Oracle Ÿ�� BULK MODE ������ sqlldr �� ����ϹǷ�, Oracle ���� connection ����
					// this.oraConnObj.rollback();
					
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
										
					this.jobRunPol.setStopJobFlag(true);
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
					
					FinishJob();
					
					return;
					
					
				}
					
				if( dataQ.size() == 0 )
				{
					// Recv Thread�� ���´� Recv Thread�� ������ ����� �޽����� setting �ϵ��� �Ǿ� �ִµ�, 
					// �̸� ������ check ����. 
					if ( this.jobRunPol.getErrMsg_Recv() != null ) {
						// RECV Thread ������ �����
						
						errMsg = "Recv Thread is stopped abnormal : " + this.jobRunPol.getCondWhere();
						XLLogger.outputInfoLog(this.logHead + errMsg);
						
						// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� - comment
						// Oracle Ÿ�� BULK MODE ������ sqlldr �� ����ϹǷ�, Oracle ���� connection ����
						// this.oraConnObj.rollback();
											
						this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
						
						
						this.jobRunPol.setStopJobFlag(true);
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
						FinishJob();
						
						return;
						
					}
					
					// cksohn - BULK mode oracle sqlldr
					if ( this.jobRunPol.getErrMsg_Loader() != null ) {
						// Loader Thread ������ �����
						
						errMsg = "Loader Thread is stopped abnormal : " + this.jobRunPol.getCondWhere();
						XLLogger.outputInfoLog(this.logHead + errMsg);
						
						// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� - comment
						// Oracle Ÿ�� BULK MODE ������ sqlldr �� ����ϹǷ�, Oracle ���� connection ����
						// this.oraConnObj.rollback();;
											
						this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
						
						
						this.jobRunPol.setStopJobFlag(true);
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
						FinishJob();
						
						return;
					}

					
					//XLLogger.outputInfoLog("CKSOHN DEBUG----- this.jobRunPol.isAliveRecvBulkThread() " + this.jobRunPol.isAliveRecvBulkThread());
					//XLLogger.outputInfoLog("CKSOHN DEBUG----- this.loaderThread.isAlive() " + this.loaderThread.isAlive());
					
					
					// ��� �۾��� ���� ���Ῡ�� check
					
					//boolean loaderAlive = true;
					//try { loaderAlive = this.loaderThread.isAlive(); } catch (Exception ee) { loaderAlive = false; }
					// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
					boolean loaderAlive = this.jobRunPol.isRunLoader();
					if ( XLConf.XL_DEBUG_YN ) {
						//XLLogger.outputInfoLog("[DEBUG] loaderAlive == " + loaderAlive);
					}
					
					if ( !this.jobRunPol.isAliveRecvBulkThread() &&  !loaderAlive) {
						
						// ErrMsg�� ���� ���¿��� Recv�� Loader Thread�� ����Ǿ��ٸ�, �ϴ� ���� ����� ó��
						XLLogger.outputInfoLog(this.logHead + " All Job Thread is finished.");
						
						
						// ���� commt �Ǽ�
						// this.totalCommitCnt += this.applyCnt;
						// this.totalApplyCnt += this.applyCnt;
						// cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
						// gssg - xl m2m bulk mode ����
						// String bulkLoadedCnt = getBulkLoadedCnt();
						// String bulkLoadedCnt = "10:0";
						// gssg - xl m2m bulk mode logging ����
						long bulkLoadedCnt = jobRunPol.getApplyCnt(); // gssg - xl recvCnt�� applyCnt�� ����
						
						// if ( bulkLoadedCnt != null ) {
							// StringTokenizer st = new StringTokenizer(bulkLoadedCnt, ":");
							
							// this.totalCommitCnt  = Long.parseLong(st.nextToken());
							this.totalCommitCnt  = bulkLoadedCnt;
							this.totalApplyCnt   = this.totalCommitCnt;
							// this.failedCnt = Long.parseLong(st.nextToken());
							this.failedCnt = 0;
						// }
						
						// gssg - xl m2m bulk mode logging ����
						// gssg - ���ʿ� ��� �ּ� ó��
						// XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt + " / " + this.jobRunPol.getPolName());
						XLLogger.outputInfoLog(this.logHead + " Policy Name : " + this.jobRunPol.getPolName());
						this.applyCnt = 0; // �ʱ�ȭ
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
						// cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
						// gssg - xl m2m bulk mode logging ����
//						if ( bulkLoadedCnt == null || (this.failedCnt != 0 ) ) { // gssg - ���ʿ� ��� �ּ� ó��
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
			
			// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� - comment
			// Oracle Ÿ�� BULK MODE ������ sqlldr �� ����ϹǷ�, Oracle ���� connection ����
			// this.oraConnObj.rollback();
			
			// ��ó��
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			
			FinishJob();
			
		} finally {
			
			// gssg - xl m2m bulk mode ����
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			// gssg - xl o2p bulk mode ����
			// gssg - mysql -> ppas ����
			try { if ( this.ppasConnObj != null ) this.ppasConnObj.closeConnection(); } catch (Exception e) {} finally { this.ppasConnObj = null; }
			try { if ( this.cataConn != null ) this.cataConn.close(); } catch (Exception e) {} finally { this.cataConn = null; }

		
			
			if ( XLConf.XL_DEBUG_YN ) {
				// gssg - xl m2m bulk mode ����
				// XLLogger.outputInfoLog("[DEBUG] this.jobRunPol.isAliveRecvThread() = " + this.jobRunPol.isAliveRecvThread());
				XLLogger.outputInfoLog("[DEBUG] this.jobRunPol.isAliveRecvThread() = " + this.jobRunPol.isAliveRecvBulkThread());
				
			}
			// 1. Recv Thread check & interrupt -���⼭ �� ��� �ϳ� ?!?!?!?
			//if ( this.jobRunPol.isAliveRecvThread() ) {
				// XLLogger.outputInfoLog(this.logHead + "[FINISH JOB] RecvThread is still alive. stop RecvThread");
			
				// this.jobRunPol.stopRecvThread();
				this.jobRunPol.stopRecvBulkThread();
				
				// cksohn - BULK mode oracle sqlldr
				try { if ( this.loaderThread != null ) this.loaderThread.interrupt(); } catch (Exception ee) {} finally { this.loaderThread = null; }
				
			//}
			
			// �޸� ������ ���⼭!!!!!
			XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName());
						
			}
		// }
		
	}
	
	
	
	// JOB ����
	private void FinishJob()
	{
		try {
			
			// XLLogger.outputInfoLog("[FINISH JOB] START - " + this.jobRunPol.getPolName());
			// cksohn - xl - ������ status log �� �α��ϵ���
			XLLogger.outputInfoLog("[FINISH JOB][" + this.jobRunPol.getPolName() + "] totalCommitCnt : " + this.totalCommitCnt);
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			// 1. Recv Thread check & interrupt -���⼭�� �̰��ϸ� �ȵȴ�.
			//if ( this.jobRunPol.isAliveRecvThread() ) {
			//	XLLogger.outputInfoLog("[FINISH JOB] RecvThread is still alive. stop RecvThread");
			//	this.jobRunPol.stopRecvThread();
			// }
			
			// 2. status �� ���� �������� �� REPORT ��� ����
			//  2-1 REPORT ���̺� �������
			 
			// XLLogger.outputInfoLog("[FINISH JOB] END - " + this.jobRunPol.getPolName());
			// cksohn - xl - ������ status log �� �α��ϵ��� start - [
			String resultStatus = "SUCCESS";
			if ( this.jobRunPol.getJobStatus().equals(XLOGCons.STATUS_FAIL) ) {
				resultStatus = "FAIL";
			} else if ( this.jobRunPol.getJobStatus().equals(XLOGCons.STATUS_ABORT) ) {
				resultStatus = "ABORT";
			}
			// XLLogger.outputInfoLog("[FINISH JOB] END - " + this.jobRunPol.getPolName() + " - " + resultStatus);
			// cksohn - xl - ������ status log �� �α��ϵ���
			XLLogger.outputInfoLog("[FINISH JOB][" + this.jobRunPol.getPolName() + "] RESULT - " + resultStatus);
			
			// ] - end cksohn - xl - ������ status log �� �α��ϵ���
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
		}
		
	}	
	
}