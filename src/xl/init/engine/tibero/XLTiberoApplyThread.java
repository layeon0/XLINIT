package xl.init.engine.tibero;

import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Vector;

import oracle.jdbc.OraclePreparedStatement;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLOracleConnection;
import xl.init.conn.XLTiberoConnection;
import xl.init.dbmgr.XLMDBManager;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLMemInfo;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLTiberoApplyThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;
		
	private XLTiberoConnection tiberoConnObj = null;
	private PreparedStatement pstmtInsert = null;
	
	private Connection cataConn = null;
	// ayzn - XLInit ��� ����  - DB ���� ���� :: jobq, cond commit �ּ�
	//private PreparedStatement pstmtUpdateJobQCommitCnt = null;
	//private PreparedStatement pstmtUpdateCondCommitCnt = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // ���� ���࿡�� commit�� �Ǽ�(condCommitCnt) + �̹� ���࿡�� commit�� �Ǽ�(applyCnt)
	
	long totalApplyCnt = 0; // �̹� �۾������ ������ insert�� ������ �Ǽ� (�����Ǽ� �ƴ�)
	
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLTiberoApplyThread(XLJobRunPol jobRunPol) {
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
		
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting ApplyThread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);	
			// ayzn - XLInit ��� ����  - DB ���� ����: jobq, cond commit �ּ�
			//this.pstmtUpdateJobQCommitCnt = mDBMgr.getPstmtUpdateJobQCommitCnt(this.cataConn);
			//this.pstmtUpdateCondCommitCnt = mDBMgr.getPstmtUpdateCondCommitCnt(this.cataConn);
			
			
			// Oracle Connection ����
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			tiberoConnObj = new XLTiberoConnection(
					tdbInfo.getIp(), 
					tdbInfo.getDbSid(),
					tdbInfo.getUserId(),
					tdbInfo.getPasswd(),
					tdbInfo.getPort(),
					tdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			if ( !tiberoConnObj.makeConnection() ) {
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
			this.pstmtInsert = (PreparedStatement)tiberoConnObj.getConnection().prepareStatement(this.jobRunPol.getTarInsertSql());

			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
			
			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
					
					
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					this.tiberoConnObj.rollback();
										
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
					
					this.tiberoConnObj.rollback();
					
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
						
						this.tiberoConnObj.rollback();
											
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
						
						this.pstmtInsert.executeBatch(); // final sendBatch call
						this.tiberoConnObj.commit();
						
						// ���� commit �Ǽ�
						this.totalCommitCnt += this.applyCnt;
						this.totalApplyCnt += this.applyCnt;
						
						XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt + " / " + this.jobRunPol.getPolName());
						this.applyCnt = 0; // �ʱ�ȭ
						// ayzn - XLInit ��� ����  - DB ���� ���� : jobq, cond commit �ּ�
						/*
						 * this.pstmtUpdateJobQCommitCnt.setLong(1, this.totalCommitCnt);
						 * this.pstmtUpdateJobQCommitCnt.setLong(2, this.jobRunPol.getJobseq());
						 * this.pstmtUpdateJobQCommitCnt.setString(3, this.jobRunPol.getPolName());
						 * this.pstmtUpdateJobQCommitCnt.executeUpdate();
						 * 
						 * this.pstmtUpdateCondCommitCnt.setLong(1, this.totalCommitCnt);
						 * this.pstmtUpdateCondCommitCnt.setString(2, this.jobRunPol.getPolName());
						 * this.pstmtUpdateCondCommitCnt.setLong(3, this.jobRunPol.getCondSeq()); //
						 * gssg - �Ϻ� ��Ʈ��ũ �л� ó�� this.pstmtUpdateCondCommitCnt.setLong(4,
						 * this.jobRunPol.getWorkPlanSeq());
						 * 
						 * this.pstmtUpdateCondCommitCnt.executeUpdate();
						 */
						//this.cataConn.commit(); 
						
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
				
				// ������ �ݿ�
				if ( !applyTarget(vtData) ) {
					
					errMsg = "[EXCEPTION] Apply : Failed Job Apply : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					this.tiberoConnObj.rollback();
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					// TODO : JOBQ & RESULT UPDATE Fail
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
					FinishJob();
					
					return;
				}


			} // while-end
			
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead +"[EXCEPTION] " +  errMsg);
			
			this.tiberoConnObj.rollback();
			
			// ��ó��
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} finally {
			
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			try { if ( this.tiberoConnObj != null ) this.tiberoConnObj.closeConnection(); } catch (Exception e) {} finally { this.tiberoConnObj = null; }
			
			// ayzn - XLInit ��� ����  - DB ���� ���� : jobq, cond commit �ּ�
			//try { if ( this.pstmtUpdateJobQCommitCnt != null ) this.pstmtUpdateJobQCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateJobQCommitCnt = null; }
			//try { if ( this.pstmtUpdateCondCommitCnt != null ) this.pstmtUpdateCondCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateCondCommitCnt = null; }

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
			// ayzn - XLInit ��� ����  - DB ���� ���� : jobseq ����
			//XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName(), this.jobRunPol.getJobseq());
			XLMemInfo.removeRJobPolInfo(this.jobRunPol.getPolName());
			
			
			XLInit.POLLING_EVENTQ.notifyEvent();
			
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
					
					if ( value == null || value.equals("")) {
						
						this.pstmtInsert.setNull(setIdx, java.sql.Types.NULL);
						
					} else {
						// ������ Ÿ�Ժ� �ݿ�
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						
//						// binary Ÿ�� (hex-->binary)
//						case XLDicInfoCons.RAW:
//						
//							byte[] bAry = XLUtil.hexToByteArray(value);
//							this.pstmtInsert.setBytes(setIdx, bAry);
//							break;
							
						// LONG/LONGRAW �� LOB Ÿ���� ���߿� ó��....
						
						// cksohn - xlim LOB Ÿ�� ���� start - [
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.NCLOB:
						case XLDicInfoCons.LONG:
						case XLDicInfoCons.XMLTYPE:
							this.pstmtInsert.setString(setIdx, value);
							break;
							
						case XLDicInfoCons.BLOB:
						case XLDicInfoCons.LONGRAW:
						// gssg - xl p2t ����
						// gssg - p2t normal mode Ÿ�� ó��
						case XLDicInfoCons.BYTEA:
						// gssg - ���������ڿ������� �������̰� ���
						// gssg - MySQL to Tibero ����
						case XLDicInfoCons.TINYBLOB:
						case XLDicInfoCons.MEDIUMBLOB:
						case XLDicInfoCons.LONGBLOB:																			
							byte[] dataBytes = XLUtil.hexToByteArray(value);
							this.pstmtInsert.setBytes(setIdx, dataBytes);
							break;
						// - end cksohn - xlim LOB Ÿ�� ����
							
						case XLDicInfoCons.DATE:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}
							
							// gssg - xl p2t ����
							// gssg - p2t normal mode Ÿ�� ó��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
								this.pstmtInsert.setString(setIdx, value);
							} else {
								// .0 ����
								value = XLUtil.removeDot(value);
								this.pstmtInsert.setString(setIdx, value);								
							}							
							break;
						
						// gssg - ���������ڿ������� �������̰� ���
						// gssg - Tibero to Tibero Ÿ���� ó��
						case XLDicInfoCons.TIMESTAMP_TZ:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							
							value = XLUtil.removeOracleTZ(value);
							
							this.pstmtInsert.setString(setIdx, value);
							break;

						// gssg - ���������ڿ������� �������̰� ���
						// gssg - Tibero to Tibero Ÿ���� ó��
						case XLDicInfoCons.TIMESTAMP_LTZ:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

								
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.ORACLE ) {
								value = XLUtil.removeOracleTZ(value);
							}
								
							this.pstmtInsert.setString(setIdx, value);
							break;
							
							
							
						// gssg - xl p2t ����
						// gssg - p2t normal mode Ÿ�� ó��
						case XLDicInfoCons.INTERVAL_YM:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

														
							// INTERVAL_YM ó��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
								// PPAS�� Tibero�� YM INTERVAL ������ ���̷� ���� ���� ���� ����
								// PPAS YM ���� : 12 years 1 mon
								// Tibero YM ���� : +12-01
								String year = value.substring(0, value.indexOf(' '));
								String month = value.substring(value.indexOf('s') + 2, value.indexOf('m') -1);
															
								if(Integer.parseInt(month) > 9){
									value = "+" + year + "-" + month;
								} else{
									value = "+" + year + "-0" + month;	
								}																													
														
							}
							
							this.pstmtInsert.setString(setIdx, value);

							break;
							
							// gssg - xl p2t ����
							// gssg - p2t normal mode Ÿ�� ó��
						case XLDicInfoCons.INTERVAL_DS:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							
							// INTERVAL_DS ó��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
							// PPAS�� Tibero�� YM INTERVAL ������ ���̷� ���� ���� ���� ����
							// PPAS DS ���� : 50 days 10:20:20.123
							// Tibero DS ���� : +50 10:20:20.123000
							// ���ǻ��� - ppas ������ day�� ������ 100�̻��� ���� �� �� ������ Ƽ���δ� 0~99������ ����
							String day;
							String second;							
									
							if (value.indexOf(' ') == -1) { // ppas ���� day ���� 0�� ��� '10:20:20.123' �� ���� �ð��� �ִ� ������ string�� �Ѿ��
								day = "0";
								second = value;
							} else {
								day = value.substring(0, value.indexOf(' '));
								second = value.substring(value.indexOf(':') - 2, value.length());
							}						
							
							if (Integer.parseInt(day) > 9) {
								value = "+" + day + " " + second;
							} else {
								value = "+0" + day + " " + second;
							}							
							}
							
							this.pstmtInsert.setString(setIdx, value);
							
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
				
				// gssg - ������ ��浿 - addBatch ó��
//				this.pstmtInsert.executeUpdate();
				this.pstmtInsert.addBatch();
								
				applyCnt++;
				
				if ( applyCnt % this.jobRunPol.getPolCommitCnt() == 0) {
					
					this.pstmtInsert.executeBatch();
					this.tiberoConnObj.commit();
					
					// ���� commit �Ǽ�
					this.totalCommitCnt += this.applyCnt;
					this.totalApplyCnt += this.applyCnt;
					XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt);
					
					this.applyCnt = 0; // �ʱ�ȭ
					
					// ayzn - XLInit ��� ����  - DB ���� ���� : jobq, cond commit �ּ�
					/*this.pstmtUpdateJobQCommitCnt.setLong(1,  this.totalCommitCnt);
					this.pstmtUpdateJobQCommitCnt.setLong(2,  this.jobRunPol.getJobseq());
					this.pstmtUpdateJobQCommitCnt.setString(3,  this.jobRunPol.getPolName());
					this.pstmtUpdateJobQCommitCnt.executeUpdate();
					
					this.pstmtUpdateCondCommitCnt.setLong(1,  this.totalCommitCnt);
					this.pstmtUpdateCondCommitCnt.setString(2,  this.jobRunPol.getPolName());
					this.pstmtUpdateCondCommitCnt.setLong(3,  this.jobRunPol.getCondSeq());
					// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
					this.pstmtUpdateCondCommitCnt.setLong(4, this.jobRunPol.getWorkPlanSeq());

					this.pstmtUpdateCondCommitCnt.executeUpdate();
					this.cataConn.commit(); */
					
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
			// }
			
			// ayzn - XLInit ��� ����  - DB ���� ���� : report, condition, jobq ���̺� ���� ó�� �ּ�
			// 2. status �� ���� �������� �� REPORT ��� ����
			//  2-1 REPORT ���̺� �������
			/*if ( !mDBMgr.insertJobResultReport(this.cataConn, this.jobRunPol, this.totalCommitCnt) ) {
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Failed to insert job result report - " + this.jobRunPol.getCondWhere());
			}
			
			//  2-2 CONDITION ���̺� STATUS update
			if ( !mDBMgr.updateJobResultCond(this.cataConn, this.jobRunPol) ) {
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Failed to update job result condition_action - " + this.jobRunPol.getCondWhere());
			}
						
			//  2-3 JOBQ ���̺� ����
			if ( !mDBMgr.deleteJobQ(this.cataConn, this.jobRunPol) ) {
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Failed to delete jobQ - " + this.jobRunPol.getCondWhere());
			}
			
			
			this.cataConn.commit();*/
		
			 
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
