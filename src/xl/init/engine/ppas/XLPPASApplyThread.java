package xl.init.engine.ppas;

import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Vector;

import com.edb.jdbc2.TimestampUtils;

import oracle.sql.INTERVALDS;
import oracle.sql.TIMESTAMPLTZ;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLPPASConnection;
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

public class XLPPASApplyThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
	// private XLOracleConnection oraConnObj = null;
	private XLPPASConnection ppasConnObj = null; // cksohn - xl o2p ��� �߰�
	
	// private OraclePreparedStatement pstmtInsert = null;
	// cksohn - xl o2p ��� �߰�
	private PreparedStatement pstmtInsert = null;
	
	private Connection cataConn = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // ���� ���࿡�� commit�� �Ǽ�(condCommitCnt) + �̹� ���࿡�� commit�� �Ǽ�(applyCnt)
	
	long totalApplyCnt = 0; // �̹� �۾������ ������ insert�� ������ �Ǽ� (�����Ǽ� �ƴ�)
	
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLPPASApplyThread(XLJobRunPol jobRunPol) {
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
			 
			
			// Oracle Connection ����
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			// oraConnObj = new XLOracleConnection(
			ppasConnObj = new XLPPASConnection( // cksohn - xl o2p ��� �߰�
					tdbInfo.getIp(), 
					tdbInfo.getDbSid(),
					tdbInfo.getUserId(),
					tdbInfo.getPasswd(),
					tdbInfo.getPort(),
					tdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			if ( !ppasConnObj.makeConnection() ) {
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
			// cksohn - xl o2p ��� �߰�
			this.pstmtInsert = (PreparedStatement)ppasConnObj.getConnection().prepareStatement(this.jobRunPol.getTarInsertSql());

			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
			
			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
					
					
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// this.oraConnObj.rollback();
					this.ppasConnObj.rollback(); // cksohn - xl o2p ��� �߰�
										
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
					
					// this.oraConnObj.rollback();
					this.ppasConnObj.rollback(); // cksohn - xl o2p ��� �߰�
					
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
						this.ppasConnObj.rollback(); // cksohn - xl o2p ��� �߰�
											
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
						// cksohn - xl o2p ��� �߰�
						this.pstmtInsert.executeBatch(); // final sendBatch call
						
						// this.oraConnObj.commit();
						this.ppasConnObj.commit(); // cksohn - xl o2p ��� �߰�
						
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
				
				// ������ �ݿ�
				if ( !applyTarget(vtData) ) {
					
					errMsg = "[EXCEPTION] Apply : Failed Job Apply : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// this.oraConnObj.rollback();
					this.ppasConnObj.rollback(); // cksohn - xl o2p ��� �߰�
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					// TODO : JOBQ & RESULT UPDATE Fail
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
					FinishJob();
					
					return;
				}


			} // while-end
			
		} catch (BatchUpdateException be) { // cksohn - xl o2p ��� �߰�
			be.getNextException().printStackTrace();
			
			XLException.outputExceptionLog(be.getNextException());
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = be.getNextException().getMessage().toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead +"[EXCEPTION] " +  errMsg);
			
			// this.oraConnObj.rollback();
			this.ppasConnObj.rollback(); // cksohn - xl o2p ��� �߰�
			
			// ��ó��
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead +"[EXCEPTION] " +  errMsg);
			
			// this.oraConnObj.rollback();
			this.ppasConnObj.rollback(); // cksohn - xl o2p ��� �߰�
			
			// ��ó��
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} finally {
			
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			
			// try { if ( this.oraConnObj != null ) this.oraConnObj.closeConnection(); } catch (Exception e) {} finally { this.oraConnObj = null; }
			// cksohn - xl o2p ��� �߰�
			try { if ( this.ppasConnObj != null ) this.ppasConnObj.closeConnection(); } catch (Exception e) {} finally { this.ppasConnObj = null; }
	
			
			
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
					
					if ( value == null ) {
						
						// this.pstmtInsert.setNull(setIdx, java.sql.Types.NULL);
						// cksohn - xl o2p ��� �߰� start - [
						// ##############################################################################
						// !!!!! �ҽ��� Oracle�� �ƴ� ���, NULL ���� ���� ���� üũ�� �޶���!!!!!!!!!!!!!!!!!!!!!!
						//       ���� �ҽ� DB �� Type�� ���� NULL check ��� ���� �ʿ�
						// ##############################################################################
						// ������ Ÿ�Ժ� �ݿ�
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						
							case XLDicInfoCons.FLOAT:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.FLOAT);
								break;
							case XLDicInfoCons.INT:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.NUMERIC); // for MSSQL
								break;
							case XLDicInfoCons.SMALLINT:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.SMALLINT);
								break;
							case XLDicInfoCons.BIGINT:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.BIGINT);
								break;
							case XLDicInfoCons.NUMBER:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.NUMERIC);
								break;
		
							case XLDicInfoCons.NUMERIC:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.NUMERIC);
								break;
		
							case XLDicInfoCons.DECIMAL:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.INTEGER); // for MSSQL
								break;
									
							case XLDicInfoCons.BINARY_DOUBLE:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.DOUBLE); 
								break;
		
							case XLDicInfoCons.BINARY_FLOAT:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.FLOAT);
								break;
							
							// gssg - xl p2p ����
							// gssg - p2p normal mode Ÿ�� ó��
							case XLDicInfoCons.DOUBLEPRECISION:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.DOUBLE);
								break;
															
							// �Ʒ� �߰� 
							case XLDicInfoCons.REAL:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.REAL);
								break;
								
							case XLDicInfoCons.TEXT:
							case XLDicInfoCons.NTEXT:							
								this.pstmtInsert.setNull(setIdx, java.sql.Types.VARCHAR);
								break;
								
							// gssg - ��ü������ ����_start_20221101
							// gssg - potgresql to postgresql timestamp_tz type ó��
							case XLDicInfoCons.TIMESTAMP:				// cksohn - for ppas - setByType
							case XLDicInfoCons.TIMESTAMP_TZ:
							case XLDicInfoCons.TIMESTAMP_LTZ:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.TIMESTAMP);
								break;																
																
							case XLDicInfoCons.DATE:				// cksohn - for ppas - setByType
								this.pstmtInsert.setNull(setIdx, java.sql.Types.DATE);
								break;
								
							// cksohn - for ppas - setByType
							case XLDicInfoCons.BINARY:
							case XLDicInfoCons.VARBINARY:
							case XLDicInfoCons.GEOGRAPHY: // maybe LOB
							case XLDicInfoCons.GEOMETRY: // maybe LOB
							case XLDicInfoCons.HIERARCHYID:  // ??
							case XLDicInfoCons.IMAGE: // maybe LOB
								
							case XLDicInfoCons.BYTEA: // cksohn - for ppas sepcial data type	
							case XLDicInfoCons.RAW: // cksohn - for otop - RAW to BYTEA
								
								this.pstmtInsert.setNull(setIdx, java.sql.Types.BINARY);
								break;
								
							// cksohn - ppas �뷮 ��ġ�� invalid byte sequence for encoding "UTF8"
							case XLDicInfoCons.CHAR:	
							case XLDicInfoCons.VARCHAR:
							case XLDicInfoCons.VARCHAR2:
							case XLDicInfoCons.NVARCHAR:
							case XLDicInfoCons.NVARCHAR2:
								
								this.pstmtInsert.setNull(setIdx, java.sql.Types.VARCHAR);		
								break;
							default:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.NULL);
								break;
									
		
						} // switch-end
											
						// - end cksohn - xl o2p ��� �߰�
						
					} else {
						// ������ Ÿ�Ժ� �ݿ�
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						
						
						// cksohn - xl o2p ��� �߰� start - [
						case XLDicInfoCons.FLOAT:						
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value)); // cksohn - for otop - FLOAT�� ���� ������ �߻���. Double.valueOf �� �ϸ� ���� ������
							break;
						case XLDicInfoCons.INT:
						case XLDicInfoCons.SMALLINT:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setInt(setIdx, Integer.valueOf(value));  
							break;
							
						case XLDicInfoCons.BIGINT:				
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setLong(setIdx, Long.valueOf(value));
							break;
							
							
						case XLDicInfoCons.NUMBER:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							// _opstmt.setDouble(_setCnt, Double.valueOf(_value));
							// cksohn - for otop - update ���� ���� ����							

//							try {
//								this.pstmtInsert.setLong(setIdx, Long.valueOf(value));
//							} catch (NumberFormatException ne ) {
//								// XLLogger.outputInfoLog("[WARN]  invalid NUMBER Type value " + ne.toString() );
//								this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
//							}
							// cksohn - xl o2p ��� �߰�
							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
//							if (value.contains(".")) {
//								this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
//							} else {
//								this.pstmtInsert.setLong(setIdx, Long.valueOf(value));
//							}
							break;

						case XLDicInfoCons.NUMERIC:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;

						case XLDicInfoCons.DECIMAL:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value)); // for MSSQL
							break;

						case XLDicInfoCons.BINARY_DOUBLE:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value)); // FLOAT
							break;

						case XLDicInfoCons.BINARY_FLOAT:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;
						
						// gssg - xl p2p ����
						// gssg - p2p normal mode Ÿ�� ó��
						case XLDicInfoCons.DOUBLEPRECISION:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;
													
						// �Ʒ� �߰� 
						case XLDicInfoCons.REAL:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;							
							
						case XLDicInfoCons.TEXT:
						case XLDicInfoCons.NTEXT:
						
							this.pstmtInsert.setString(setIdx, value);
							break;
							
						case XLDicInfoCons.TIMESTAMP:				// cksohn - for ppas - setByType
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
							break;														

						case XLDicInfoCons.TIMESTAMP_TZ:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL || this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS ) {
								
								int tzIdx = 0;
								
								if ( value.indexOf("+") > -1 ) {
									tzIdx = value.lastIndexOf("+");
								} else {
									tzIdx = value.lastIndexOf("-");									
								}								
								
								value = value.substring(0, tzIdx);
								
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								
								
							} else {

								// O2P���� TIMEZONE�� ó�� �ȵ�. ����, Ÿ���� VARCHAR Ÿ�Ե����� �����Ͽ� ó���ϰų�, 
							    // �������� ���ؼ��� Asia/Seoul or +09:00 �κ��� ©�� �ݿ��ϵ��� �Ѵ�. 														

								int tzIdx = value.lastIndexOf(" ");
								value = value.substring(0, tzIdx);
																
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								
							}
							break;
						
						case XLDicInfoCons.TIMESTAMP_LTZ:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL || this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS ) {
								
								int tzIdx = 0;
								
								if ( value.indexOf("+") > -1 ) {
									tzIdx = value.lastIndexOf("+");
								} else {
									tzIdx = value.lastIndexOf("-");									
								}								
								
								value = value.substring(0, tzIdx);
								
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								
								
							} else {

								// O2P���� TIMEZONE�� ó�� �ȵ�. ����, Ÿ���� VARCHAR Ÿ�Ե����� �����Ͽ� ó���ϰų�, 
							    // �������� ���ؼ��� Asia/Seoul or +09:00 �κ��� ©�� �ݿ��ϵ��� �Ѵ�. 							

								// gssg - ���������ڿ������� �������̰� ���
								// gssg - t2p ltz ó��								
								if ( this.jobRunPol.getSdbInfo().getDbType() != XLCons.TIBERO ) {
									int tzIdx = value.lastIndexOf(" ");
									value = value.substring(0, tzIdx);																		
								}
								
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								
							}
							break;					
							
						case XLDicInfoCons.DATE:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}
							
							// gssg - t2p ����
							// gssg - t2b normal mode date type ó��
							// TIBERO, ORACLE - PostgreSQL : DATE - TIMESTAMP(0)
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.ORACLE || this.jobRunPol.getSdbInfo().getDbType() == XLCons.TIBERO ) {
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								break;
								
							} else {
								// gssg - ��ü������ ����_start_20221101
								// gssg - potgresql to postgresql date type ó��
								this.pstmtInsert.setDate(setIdx, Date.valueOf(value));							
								break;								
							}
						
						case XLDicInfoCons.INTERVAL_DS: // cksohn - for ppas sepcial data type
						case XLDicInfoCons.INTERVAL_YM: // cksohn - for ppas sepcial data type
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							// cksohn - for otop & for ppas - INTERVAL TYPE ���� ����
							// _opstmt.setObject(_setCnt, new PGInterval(_value));
							this.pstmtInsert.setString(setIdx, value);
							break;
							
						case XLDicInfoCons.BINARY: 
						case XLDicInfoCons.VARBINARY:
						case XLDicInfoCons.GEOGRAPHY: // maybe LOB
						case XLDicInfoCons.GEOMETRY: // maybe LOB
						case XLDicInfoCons.HIERARCHYID:  // ??
						case XLDicInfoCons.IMAGE: // maybe LOB
						case XLDicInfoCons.BLOB: // cksohn - xl o2p ��� �߰�
						case XLDicInfoCons.LONGRAW: // cksohn - for otop - LOB type ���� - Oracle LONGRAW
							
//							NRAPManager.outputInfoLog("CKSOHN DEBUG:::: setBytes()!!!");
							this.pstmtInsert.setBytes(setIdx, XLUtil.hexToByteArray(value));
							break;

						case XLDicInfoCons.BYTEA:
						case XLDicInfoCons.RAW: // cksohn - for otop - RAW to BYTEA							
							// cksohn - for otop - RAW to BYTEA
							// OtoP ������ �׳� BYTEA �� �ٲپ �ϸ� �ȴ�. 
							this.pstmtInsert.setBytes(setIdx, XLUtil.hexToByteArray(value));
							
							// �Ʒ��� PtoP�ϰ��.
//							// x414141 �� ���� ���·� �Ѿ��.
//							if ( _value.length() > 1) {
//								_opstmt.setBytes(_setCnt, NRAPUtil.hexToByteArray(_value.substring(1, _value.length())));
//							} else { // '\x' �� ������ ��� ����.
//								String emptyStr = "";
//								_opstmt.setBytes(_setCnt, emptyStr.getBytes());
//							}
							break;
							
						default:
							
							// gssg - �����Ϸ��� O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setString(setIdx, value);
							break;
								
						
						
						
						// - end cksohn - xl o2p ��� �߰�
							

						}						
					}
				} // for-end (j) - 1���� record �ݿ�
				
				// this.pstmtInsert.executeUpdate();
				// cksohn - xl o2p ��� �߰�
				this.pstmtInsert.addBatch();
				
				applyCnt++;
				
				if ( applyCnt % this.jobRunPol.getPolCommitCnt() == 0) {
					
					// this.pstmtInsert.sendBatch();
					// cksohn - xl o2p ��� �߰�
					this.pstmtInsert.executeBatch();
					
					// this.oraConnObj.commit();
					this.ppasConnObj.commit(); // cksohn - xl o2p ��� �߰�
					
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
			
		} catch (BatchUpdateException be) { // cksohn - xl o2p ��� �߰�
			
			be.getNextException().printStackTrace();
			return false;
			
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
