package xl.init.engine.mssql;

// gssg - ms2ms ����

import java.io.RandomAccessFile;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

import oracle.jdbc.OraclePreparedStatement;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLMSSQLConnection;
import xl.init.conn.XLMariaDBConnection;
import xl.init.conn.XLOracleConnection;
import xl.init.dbmgr.XLMDBManager;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfo;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLMemInfo;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLMSSQLApplyThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
//	private XLOracleConnection oraConnObj = null;
	// gssg - xl o2m ����
//	private XLMariaDBConnection mariaDBConnObj = null; // gssg - xl m2m ��� �߰�  - 0413
	private XLMSSQLConnection mssqlConnObj = null;
	
//	private OraclePreparedStatement pstmtInsert = null;
	private PreparedStatement pstmtInsert = null; // gssg - xl m2m ��� �߰�  - 0413
	
	private Connection cataConn = null;
	private PreparedStatement pstmtUpdateJobQCommitCnt = null;
	private PreparedStatement pstmtUpdateCondCommitCnt = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // ���� ���࿡�� commit�� �Ǽ�(condCommitCnt) + �̹� ���࿡�� commit�� �Ǽ�(applyCnt)
	
	long totalApplyCnt = 0; // �̹� �۾������ ������ insert�� ������ �Ǽ� (�����Ǽ� �ƴ�)
	
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLMSSQLApplyThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		this.totalCommitCnt = this.jobRunPol.getCondCommitCnt();
				
		// ��� ���̺��� �÷� ����
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][APPLY]";
	}


	@Override
	public void run(){
		
		// gssg - ms2ms ����
		// gssg - IDENTITY �÷� ó��
		Statement st = null;
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
				
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting ApplyThread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			// gssg - xl o2m ����
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);			
			this.pstmtUpdateJobQCommitCnt = mDBMgr.getPstmtUpdateJobQCommitCnt(this.cataConn);
			this.pstmtUpdateCondCommitCnt = mDBMgr.getPstmtUpdateCondCommitCnt(this.cataConn);
			 
			
			// Oracle Connection ����
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			// oraConnObj = new XLOracleConnection(
			// gssg - xl m2m ��� �߰�  - 0413
			// gssg - ms2ms ����
			// gssg - IDENTITY �÷� ó��
			mssqlConnObj = new XLMSSQLConnection( // gssg - xl m2m ��� �߰�  - 0413
					tdbInfo.getIp(), 
					tdbInfo.getDbSid(),
					tdbInfo.getUserId(),
					tdbInfo.getPasswd(),
					tdbInfo.getPort(),
					tdbInfo.getDbType(),
					true
					);
			
			
			// Target DB Connection
			if ( !mssqlConnObj.makeConnection() ) {
				errMsg = "[EXCEPTION] Apply : Failed to make target db connection - " + tdbInfo.getIp() + "/" + tdbInfo.getDbSid(); 
				XLLogger.outputInfoLog(this.logHead + errMsg);
				// TODO ���⼭ Catalog DB�� ���з� update ġ�� ������ �ϴµ�,, catalog �� Ÿ�ٿ� ���� ��� ������ �Ǳ���. 
				//      �׷���, ���� ����� �����ϰ� JOBQ�� ����� �����ϵ��� ��ġ�ؾ� �� ���� ����. 
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead +"[EXCEPTION] Apply Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Apply(errMsg);
				return;
			} else {
				XLLogger.outputInfoLog(this.logHead + " Target DBMS is connected - " +  tdbInfo.getIp() + "/" + tdbInfo.getDbSid());
			}

			// Target �ݿ� insert preparedStatement ���� ����
			
			// this.pstmtInsert = (OraclePreparedStatement)oraConnObj.getConnection().prepareStatement(this.jobRunPol.getTarInsertSql());
			// gssg - xl m2m ��� �߰�  - 0413
			
			
			// gssg - xl īŻ�α� mariadb
			this.pstmtInsert = (PreparedStatement)mssqlConnObj.getConnection().prepareStatement(this.jobRunPol.getTarInsertSql());

			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
			
			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
					
					
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// this.oraConnObj.rollback();
					this.mssqlConnObj.rollback(); // gssg - xl m2m ��� �߰�  - 0413
										
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
					
					// this.oraConnObj.rollback();
					this.mssqlConnObj.rollback(); // gssg - xl m2m ��� �߰�  - 0413
					
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
						
						// this.oraConnObj.rollback();
						this.mssqlConnObj.rollback(); // gssg - xl m2m ��� �߰�  - 0413
											
						this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
						
						
						this.jobRunPol.setStopJobFlag(true);
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
						FinishJob();
						
						return;
						
					}
					
					isWait = true;
					dataQ.waitDataQ();
					continue;
				}
				
				isWait = false;
				
				//Queue���� Apply�� Object�� ������ �´�. 
				vtData = dataQ.getDataQ();
				
				//Queue���� �����Ѵ�.
				dataQ.removeDataQ();
				
			
				// XLLogger.outputInfoLog("CKSOHN DEBUG  vtData.size ---> " + vtData.size());
				// EOF check
				if ( vtData.size() == 1) {
					
					
					ArrayList<String> recordDataArray = vtData.get(0);
					
					if ( recordDataArray == null ) {
						
						// this.pstmtInsert.sendBatch(); // final sendBatch call
						// gssg - xl m2m ��� �߰�
						this.pstmtInsert.executeBatch(); // final sendBatch call
						
						// this.oraConnObj.commit();
						this.mssqlConnObj.commit(); // gssg - xl m2m ��� �߰�  - 0413
						
						// ���� commt �Ǽ�
						this.totalCommitCnt += this.applyCnt;
						this.totalApplyCnt += this.applyCnt;
						
						XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt + " / " + this.jobRunPol.getPolName());
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
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_SUCCESS);
						FinishJob();
						
						return;
					}
				
				}
				
				// gssg - ms2ms ����
				// gssg - IDENTITY �÷� ó��
//				XLLogger.outputInfoLog("[APPLY] SET IDENTITY_INSERT");
//				XLLogger.outputInfoLog("[APPLY] vt size = " + this.mssqlConnObj.getIdentityVt().size());
//				XLLogger.outputInfoLog("[APPLY] ttable = " + this.jobRunPol.getTableInfo().getTtable());
				
//				[22/11/04 17:34:46] [APPLY] ht = [{TB_TNAME=MS_IDENTITY2}, {TB_TNAME=MS_IDENTITY}]
//				[22/11/04 17:34:46] [APPLY] ttable = MS_IDENTITY
				
//				XLLogger.outputInfoLog("[APPLY] vt = " + this.mssqlConnObj.getIdentityVt());
												
				Hashtable<String, String> tempHash = new Hashtable<String, String>();
				tempHash.put("TB_TNAME", this.jobRunPol.getTableInfo().getTtable());
				if ( this.mssqlConnObj.getIdentityVt().contains(tempHash) ) {

//					XLLogger.outputInfoLog("########################################");
//					XLLogger.outputInfoLog("THIS IS CONTAINS TRUE POINT");
//					XLLogger.outputInfoLog("########################################");

					st = this.mssqlConnObj.conn.createStatement();
					st.executeUpdate("SET IDENTITY_INSERT " + this.jobRunPol.getTableInfo().getTowner() + "." + this.jobRunPol.getTableInfo().getTtable() +" ON");					
					
					try{if(st != null) st.close();}catch(Exception e){}finally{st = null;}
				}


				
				// ������ �ݿ�
				if ( !applyTarget(vtData) ) {
					
					errMsg = "[EXCEPTION] Apply : Failed Job Apply : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// this.oraConnObj.rollback();
					this.mssqlConnObj.rollback(); // gssg - xl m2m ��� �߰�  - 0413
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					// TODO : JOBQ & RESULT UPDATE Fail
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
					FinishJob();
					
					return;
				}

			} // while-end
			

			
		}  catch (BatchUpdateException be) { // gssg - xl m2m ��� �߰�
			be.printStackTrace();
//			be.getNextException().printStackTrace();
			
			// XLException.outputExceptionLog(be.getNextException());
			this.jobRunPol.setStopJobFlag(true);
			
			// errMsg = be.getNextException().getMessage().toString();
			errMsg = be.getMessage().toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead + "[EXCEPTION] " +  errMsg);
			// this.oraConnObj.rollback();
			this.mssqlConnObj.rollback(); // gssg - xl m2m ��� �߰�  - 0413
			
			// ��ó��
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} catch (Exception e) {
			// e.printStackTrace();
			XLException.outputExceptionLog(e);
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead +"[EXCEPTION] " +  errMsg);
			
			// this.oraConnObj.rollback();
			this.mssqlConnObj.rollback(); // gssg - xl m2m ��� �߰�  - 0413
			
			// ��ó��
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} finally {
			
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			// try { if ( this.oraConnObj != null ) this.oraConnObj.closeConnection(); } catch (Exception e) {} finally { this.oraConnObj = null; }
			// gssg - xl m2m ��� �߰�  - 0413
			try { if ( this.mssqlConnObj != null ) this.mssqlConnObj.closeConnection(); } catch (Exception e) {} finally { this.mssqlConnObj = null; }
			
			try { if ( this.pstmtUpdateJobQCommitCnt != null ) this.pstmtUpdateJobQCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateJobQCommitCnt = null; }
			try { if ( this.pstmtUpdateCondCommitCnt != null ) this.pstmtUpdateCondCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateCondCommitCnt = null; }
			try { if ( this.cataConn != null ) this.cataConn.close(); } catch (Exception e) {} finally { this.cataConn = null; }
			
			
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] this.jobRunPol.isAliveRecvThread() = " + this.jobRunPol.isAliveRecvThread());
			}
			// 1. Recv Thread check & interrupt -���⼭ �� ��� �ϳ� ?!?!?!?
			//if ( this.jobRunPol.isAliveRecvThread() ) {
				// XLLogger.outputInfoLog(this.logHead + "[FINISH JOB] RecvThread is still alive. stop RecvThread");
				this.jobRunPol.stopRecvThread();
			//}
			
			// �޸� ������ ���⼭!!!!!
			XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName());
			
			
			//XLInit.POLLING_EVENTQ.notifyEvent();
		}
		
	}
	
	
	
	private boolean applyTarget(Vector<ArrayList<String>> _vtData)
	{
		try {
			

			for (int i=0; i<_vtData.size(); i++) {
				
				ArrayList<String> recordDataArray = _vtData.get(i);
				
				for (int j=0; j<recordDataArray.size(); j++) {
					
					int setIdx = j+1;
					
					String value = recordDataArray.get(j);
					
					// if ( value == null || value.equals("")) {
					// gssg - xl m2m ��� �߰�
					if ( value == null) {

						// this.pstmtInsert.setNull(setIdx, java.sql.Types.NULL);
						// gssg - xl m2m ��� �߰� start - [
						// ##############################################################################
						// !!!!! �ҽ��� Oracle�� �ƴ� ���, NULL ���� ���� ���� üũ�� �޶���!!!!!!!!!!!!!!!!!!!!!!
						//       ���� �ҽ� DB �� Type�� ���� NULL check ��� ���� �ʿ�
						// ##############################################################################
						// ������ Ÿ�Ժ� �ݿ�

						// gssg - ms2ms ����
						// gssg - null ó��
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						case XLDicInfoCons.DATE:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.DATE);
							break;
						// gssg - LG���� MS2O
						// gssg - ms2ms normal mode ����
						case XLDicInfoCons.DATETIME:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.DATE);
							break;
						// gssg - �����Ϸ��� O2MS
						case XLDicInfoCons.DATETIME2:
						case XLDicInfoCons.TIMESTAMP:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.DATE);
							break;
						// gssg - �����Ϸ��� O2MS
						case XLDicInfoCons.DATETIMEOFFSET:
						case XLDicInfoCons.TIMESTAMP_TZ:
						case XLDicInfoCons.TIMESTAMP_LTZ:						
							this.pstmtInsert.setNull(setIdx, java.sql.Types.DATE);
							break;
						// gssg - �����Ϸ��� O2MS
						case XLDicInfoCons.TEXT:
						case XLDicInfoCons.NTEXT:
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.NCLOB:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.VARCHAR);
							break;
						case XLDicInfoCons.REAL:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.REAL);
							break;
						case XLDicInfoCons.TIME:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.TIME);
							break;
						// gssg - �����Ϸ��� O2MS
						case XLDicInfoCons.FLOAT:
						case XLDicInfoCons.NUMBER:
						case XLDicInfoCons.BINARY_DOUBLE:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.FLOAT);
							break;

						default:
							this.pstmtInsert.setNull(setIdx, java.sql.Types.NULL);
							break;
						}
						
						
					} else {
						// ������ Ÿ�Ժ� �ݿ�
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						
						// gssg - ms2ms ����
						// gssg - ���̳ʸ� Ÿ�� ó��
						// gssg - �����Ϸ��� O2MS
						case XLDicInfoCons.BINARY:
						case XLDicInfoCons.VARBINARY:
						case XLDicInfoCons.IMAGE:
						case XLDicInfoCons.BLOB:
						case XLDicInfoCons.LONGRAW:
						case XLDicInfoCons.RAW:
							
							byte[] bAry = XLUtil.hexToByteArray(value);
							this.pstmtInsert.setBytes(setIdx, bAry);
							break;
						
						// gssg - �����Ϸ��� O2MS							
						case XLDicInfoCons.TIMESTAMP_LTZ:
							// O2MS���� TIMEZONE�� ó�� �ȵ�. ����, 
						    // �������� ���ؼ��� Asia/Seoul or +09:00 �κ��� ©�� �ݿ��ϵ��� �Ѵ�. 							

							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							int tzIdx = value.lastIndexOf(" ");
							value = value.substring(0, tzIdx);
															
							this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));

							break;							
							
						default :
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setString(setIdx, value);
							
							break;
							
						}						
					}
				} // for-end (j) - 1���� record �ݿ�
				
				
				
				// gssg - xl m2m ���� ���� ����- PPAS Apply ���� executeUpdate --> addBatch
				// this.pstmtInsert.executeUpdate();
				// gssg - xl m2m ��� �߰�
				this.pstmtInsert.addBatch();
				
				applyCnt++;
				
				if ( applyCnt % this.jobRunPol.getPolCommitCnt() == 0) {
					
					// gssg - xl m2m ���� ���� ����- PPAS Apply ���� sendBatch --> executeBatch
					// this.pstmtInsert.sendBatch();
					// gssg - xl m2m ��� �߰�
					this.pstmtInsert.executeBatch();
					
					// this.oraConnObj.commit();
					this.mssqlConnObj.commit(); // gssg - xl m2m ��� �߰�  - 0413
					
					// ���� commt �Ǽ�
					this.totalCommitCnt += this.applyCnt;
					this.totalApplyCnt += this.applyCnt;
					XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt);
					
					this.applyCnt = 0; // �ʱ�ȭ
										
					if ( XLConf.XL_DEBUG_YN ) {
						XLLogger.outputInfoLog("[DEBUG] commitCnt-1 : " + this.totalCommitCnt);
					}
					
				}
				
			} // for-end (i)
			
			
			return true;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			return false;
		}
	}
	
	
	// JOB ����
	private void FinishJob()
	{
		try {
			
			// XLLogger.outputInfoLog("[FINISH JOB] START - " + this.jobRunPol.getPolName());
			// cksohn - xl - ������ status log �� �α��ϵ���
			XLLogger.outputInfoLog("[FINISH JOB][" +  this.jobRunPol.getPolName() + "] totalCommitCnt : " + this.totalCommitCnt);
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			// 1. Recv Thread check & interrupt -���⼭�� �̰��ϸ� �ȵȴ�.
			//if ( this.jobRunPol.isAliveRecvThread() ) {
			//	XLLogger.outputInfoLog("[FINISH JOB] RecvThread is still alive. stop RecvThread");
			//	this.jobRunPol.stopRecvThread();
	
			 
			// XLLogger.outputInfoLog("[FINISH JOB] END - " + this.jobRunPol.getPolName());
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