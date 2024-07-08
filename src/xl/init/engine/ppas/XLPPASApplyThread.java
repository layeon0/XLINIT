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
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
	// private XLOracleConnection oraConnObj = null;
	private XLPPASConnection ppasConnObj = null; // cksohn - xl o2p 기능 추가
	
	// private OraclePreparedStatement pstmtInsert = null;
	// cksohn - xl o2p 기능 추가
	private PreparedStatement pstmtInsert = null;
	
	private Connection cataConn = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // 기존 수행에서 commit된 건수(condCommitCnt) + 이번 수행에서 commit된 건수(applyCnt)
	
	long totalApplyCnt = 0; // 이번 작업수행시 실제로 insert를 수행한 건수 (누적건수 아님)
	
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLPPASApplyThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		this.totalCommitCnt = this.jobRunPol.getCondCommitCnt();
				
		// 대상 테이블의 컬럼 정보
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][APPLY]";
	}


	@Override
	public void run(){
		
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting ApplyThread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			XLMDBManager mDBMgr = new XLMDBManager();
			this.cataConn = mDBMgr.createConnection(false);			
			 
			
			// Oracle Connection 생성
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			// oraConnObj = new XLOracleConnection(
			ppasConnObj = new XLPPASConnection( // cksohn - xl o2p 기능 추가
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
				// TODO 여기서 Catalog DB에 실패로 update 치고 끝나야 하는데,, catalog 가 타겟에 있을 경우 문제가 되긴함. 
				//      그러면, 추후 수행시 깨끗하게 JOBQ를 지우고 수행하도록 조치해야 할 수도 있음. 
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead +"[EXCEPTION] Apply Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Apply(errMsg);
				return;
			} else {
				XLLogger.outputInfoLog(this.logHead + " Target DBMS is connected - " +  tdbInfo.getIp() + "/" + tdbInfo.getDbSid());
			}

			// Target 반영 insert preparedStatement 구문 생성
			
			// this.pstmtInsert = (OraclePreparedStatement)oraConnObj.getConnection().prepareStatement(this.jobRunPol.getTarInsertSql());
			// cksohn - xl o2p 기능 추가
			this.pstmtInsert = (PreparedStatement)ppasConnObj.getConnection().prepareStatement(this.jobRunPol.getTarInsertSql());

			
			stime = System.currentTimeMillis();
			
			Vector<ArrayList<String>>  vtData = null;
			
			while( true )
			{
				if ( XLInit.STOP_FLAG ) {
					
					
					errMsg = "[STOP] Apply is stopped by stop request : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// this.oraConnObj.rollback();
					this.ppasConnObj.rollback(); // cksohn - xl o2p 기능 추가
										
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
					this.ppasConnObj.rollback(); // cksohn - xl o2p 기능 추가
					
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
						
						// this.oraConnObj.rollback();
						this.ppasConnObj.rollback(); // cksohn - xl o2p 기능 추가
											
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
				
				//Queue에서 Apply할 Object를 가지고 온다. 
				vtData = dataQ.getDataQ();
				
				//Queue에서 삭제한다.
				dataQ.removeDataQ();
				
				
				// XLLogger.outputInfoLog("CKSOHN DEBUG  vtData.size ---> " + vtData.size());
				// EOF check
				if ( vtData.size() == 1) {
					
					
					ArrayList<String> recordDataArray = vtData.get(0);
					
					if ( recordDataArray == null ) {
						
						// this.pstmtInsert.sendBatch(); // final sendBatch call
						// cksohn - xl o2p 기능 추가
						this.pstmtInsert.executeBatch(); // final sendBatch call
						
						// this.oraConnObj.commit();
						this.ppasConnObj.commit(); // cksohn - xl o2p 기능 추가
						
						// 누적 commt 건수
						this.totalCommitCnt += this.applyCnt;
						this.totalApplyCnt += this.applyCnt;
						
						XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt + " / " + this.jobRunPol.getPolName());
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
						this.jobRunPol.setJobStatus(XLOGCons.STATUS_SUCCESS);
						FinishJob();
						
						return;
					}
				
				}
				
				// 데이터 반영
				if ( !applyTarget(vtData) ) {
					
					errMsg = "[EXCEPTION] Apply : Failed Job Apply : " + this.jobRunPol.getCondWhere();
					XLLogger.outputInfoLog(this.logHead + errMsg);
					
					// this.oraConnObj.rollback();
					this.ppasConnObj.rollback(); // cksohn - xl o2p 기능 추가
										
					this.jobRunPol.setErrMsg_Apply(this.logHead + errMsg);
					
					// TODO : JOBQ & RESULT UPDATE Fail
					this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
					FinishJob();
					
					return;
				}


			} // while-end
			
		} catch (BatchUpdateException be) { // cksohn - xl o2p 기능 추가
			be.getNextException().printStackTrace();
			
			XLException.outputExceptionLog(be.getNextException());
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = be.getNextException().getMessage().toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Apply(this.logHead +"[EXCEPTION] " +  errMsg);
			
			// this.oraConnObj.rollback();
			this.ppasConnObj.rollback(); // cksohn - xl o2p 기능 추가
			
			// 뒷처리
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
			this.ppasConnObj.rollback(); // cksohn - xl o2p 기능 추가
			
			// 뒷처리
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} finally {
			
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			
			// try { if ( this.oraConnObj != null ) this.oraConnObj.closeConnection(); } catch (Exception e) {} finally { this.oraConnObj = null; }
			// cksohn - xl o2p 기능 추가
			try { if ( this.ppasConnObj != null ) this.ppasConnObj.closeConnection(); } catch (Exception e) {} finally { this.ppasConnObj = null; }
	
			
			
			try { if ( this.cataConn != null ) this.cataConn.close(); } catch (Exception e) {} finally { this.cataConn = null; }
			
			
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] this.jobRunPol.isAliveRecvThread() = " + this.jobRunPol.isAliveRecvThread());
			}
			// 1. Recv Thread check & interrupt -여기서 해 줘야 하나 ?!?!?!?
			//if ( this.jobRunPol.isAliveRecvThread() ) {
				// XLLogger.outputInfoLog(this.logHead + "[FINISH JOB] RecvThread is still alive. stop RecvThread");
				this.jobRunPol.stopRecvThread();
			//}
			
			// 메모리 정리는 여기서!!!!!
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
						// cksohn - xl o2p 기능 추가 start - [
						// ##############################################################################
						// !!!!! 소스가 Oracle이 아닐 경우, NULL 값에 대한 조건 체크가 달라짐!!!!!!!!!!!!!!!!!!!!!!
						//       추후 소스 DB 의 Type에 따라 NULL check 방식 변경 필요
						// ##############################################################################
						// 데이터 타입별 반영
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
							
							// gssg - xl p2p 지원
							// gssg - p2p normal mode 타입 처리
							case XLDicInfoCons.DOUBLEPRECISION:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.DOUBLE);
								break;
															
							// 아래 추가 
							case XLDicInfoCons.REAL:
								this.pstmtInsert.setNull(setIdx, java.sql.Types.REAL);
								break;
								
							case XLDicInfoCons.TEXT:
							case XLDicInfoCons.NTEXT:							
								this.pstmtInsert.setNull(setIdx, java.sql.Types.VARCHAR);
								break;
								
							// gssg - 전체적으로 보완_start_20221101
							// gssg - potgresql to postgresql timestamp_tz type 처리
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
								
							// cksohn - ppas 대량 배치시 invalid byte sequence for encoding "UTF8"
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
											
						// - end cksohn - xl o2p 기능 추가
						
					} else {
						// 데이터 타입별 반영
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						
						
						// cksohn - xl o2p 기능 추가 start - [
						case XLDicInfoCons.FLOAT:						
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value)); // cksohn - for otop - FLOAT로 인해 오차가 발생함. Double.valueOf 로 하면 오차 없어짐
							break;
						case XLDicInfoCons.INT:
						case XLDicInfoCons.SMALLINT:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setInt(setIdx, Integer.valueOf(value));  
							break;
							
						case XLDicInfoCons.BIGINT:				
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setLong(setIdx, Long.valueOf(value));
							break;
							
							
						case XLDicInfoCons.NUMBER:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							// _opstmt.setDouble(_setCnt, Double.valueOf(_value));
							// cksohn - for otop - update 성능 저하 문제							

//							try {
//								this.pstmtInsert.setLong(setIdx, Long.valueOf(value));
//							} catch (NumberFormatException ne ) {
//								// XLLogger.outputInfoLog("[WARN]  invalid NUMBER Type value " + ne.toString() );
//								this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
//							}
							// cksohn - xl o2p 기능 추가
							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
//							if (value.contains(".")) {
//								this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
//							} else {
//								this.pstmtInsert.setLong(setIdx, Long.valueOf(value));
//							}
							break;

						case XLDicInfoCons.NUMERIC:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;

						case XLDicInfoCons.DECIMAL:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value)); // for MSSQL
							break;

						case XLDicInfoCons.BINARY_DOUBLE:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value)); // FLOAT
							break;

						case XLDicInfoCons.BINARY_FLOAT:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;
						
						// gssg - xl p2p 지원
						// gssg - p2p normal mode 타입 처리
						case XLDicInfoCons.DOUBLEPRECISION:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setDouble(setIdx, Double.valueOf(value));
							break;
													
						// 아래 추가 
						case XLDicInfoCons.REAL:
							
							// gssg - 세븐일레븐 O2MS
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
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
							break;														

						case XLDicInfoCons.TIMESTAMP_TZ:
							
							// gssg - 세븐일레븐 O2MS
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

								// O2P에서 TIMEZONE은 처리 안됨. 따라서, 타겟을 VARCHAR 타입등으로 변경하여 처리하거나, 
							    // 정상복제를 위해서는 Asia/Seoul or +09:00 부분을 짤라서 반영하도록 한다. 														

								int tzIdx = value.lastIndexOf(" ");
								value = value.substring(0, tzIdx);
																
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								
							}
							break;
						
						case XLDicInfoCons.TIMESTAMP_LTZ:
							
							// gssg - 세븐일레븐 O2MS
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

								// O2P에서 TIMEZONE은 처리 안됨. 따라서, 타겟을 VARCHAR 타입등으로 변경하여 처리하거나, 
							    // 정상복제를 위해서는 Asia/Seoul or +09:00 부분을 짤라서 반영하도록 한다. 							

								// gssg - 국가정보자원관리원 데이터이관 사업
								// gssg - t2p ltz 처리								
								if ( this.jobRunPol.getSdbInfo().getDbType() != XLCons.TIBERO ) {
									int tzIdx = value.lastIndexOf(" ");
									value = value.substring(0, tzIdx);																		
								}
								
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								
							}
							break;					
							
						case XLDicInfoCons.DATE:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}
							
							// gssg - t2p 보완
							// gssg - t2b normal mode date type 처리
							// TIBERO, ORACLE - PostgreSQL : DATE - TIMESTAMP(0)
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.ORACLE || this.jobRunPol.getSdbInfo().getDbType() == XLCons.TIBERO ) {
								this.pstmtInsert.setTimestamp(setIdx, Timestamp.valueOf(value));
								break;
								
							} else {
								// gssg - 전체적으로 보완_start_20221101
								// gssg - potgresql to postgresql date type 처리
								this.pstmtInsert.setDate(setIdx, Date.valueOf(value));							
								break;								
							}
						
						case XLDicInfoCons.INTERVAL_DS: // cksohn - for ppas sepcial data type
						case XLDicInfoCons.INTERVAL_YM: // cksohn - for ppas sepcial data type
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							// cksohn - for otop & for ppas - INTERVAL TYPE 구문 변경
							// _opstmt.setObject(_setCnt, new PGInterval(_value));
							this.pstmtInsert.setString(setIdx, value);
							break;
							
						case XLDicInfoCons.BINARY: 
						case XLDicInfoCons.VARBINARY:
						case XLDicInfoCons.GEOGRAPHY: // maybe LOB
						case XLDicInfoCons.GEOMETRY: // maybe LOB
						case XLDicInfoCons.HIERARCHYID:  // ??
						case XLDicInfoCons.IMAGE: // maybe LOB
						case XLDicInfoCons.BLOB: // cksohn - xl o2p 기능 추가
						case XLDicInfoCons.LONGRAW: // cksohn - for otop - LOB type 복제 - Oracle LONGRAW
							
//							NRAPManager.outputInfoLog("CKSOHN DEBUG:::: setBytes()!!!");
							this.pstmtInsert.setBytes(setIdx, XLUtil.hexToByteArray(value));
							break;

						case XLDicInfoCons.BYTEA:
						case XLDicInfoCons.RAW: // cksohn - for otop - RAW to BYTEA							
							// cksohn - for otop - RAW to BYTEA
							// OtoP 에서는 그냥 BYTEA 로 바꾸어서 하면 된다. 
							this.pstmtInsert.setBytes(setIdx, XLUtil.hexToByteArray(value));
							
							// 아래는 PtoP일경우.
//							// x414141 과 같은 형태로 넘어옴.
//							if ( _value.length() > 1) {
//								_opstmt.setBytes(_setCnt, NRAPUtil.hexToByteArray(_value.substring(1, _value.length())));
//							} else { // '\x' 만 존재할 경우 있음.
//								String emptyStr = "";
//								_opstmt.setBytes(_setCnt, emptyStr.getBytes());
//							}
							break;
							
						default:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							this.pstmtInsert.setString(setIdx, value);
							break;
								
						
						
						
						// - end cksohn - xl o2p 기능 추가
							

						}						
					}
				} // for-end (j) - 1개의 record 반영
				
				// this.pstmtInsert.executeUpdate();
				// cksohn - xl o2p 기능 추가
				this.pstmtInsert.addBatch();
				
				applyCnt++;
				
				if ( applyCnt % this.jobRunPol.getPolCommitCnt() == 0) {
					
					// this.pstmtInsert.sendBatch();
					// cksohn - xl o2p 기능 추가
					this.pstmtInsert.executeBatch();
					
					// this.oraConnObj.commit();
					this.ppasConnObj.commit(); // cksohn - xl o2p 기능 추가
					
					// 누적 commt 건수
					this.totalCommitCnt += this.applyCnt;
					this.totalApplyCnt += this.applyCnt;
					XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt);
					
					this.applyCnt = 0; // 초기화
						
					if ( XLConf.XL_DEBUG_YN ) {
						XLLogger.outputInfoLog("[DEBUG] commitCnt-1 : " + this.totalCommitCnt);
					}
					
					
				}
				
			} // for-end (i)
			
			
			return true;
			
		} catch (BatchUpdateException be) { // cksohn - xl o2p 기능 추가
			
			be.getNextException().printStackTrace();
			return false;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			return false;
		}
	}
	
	
	// JOB 종료
	private void FinishJob()
	{
		try {
			
			// XLLogger.outputInfoLog("[FINISH JOB] START - " + this.jobRunPol.getPolName());
			
			// cksohn - xl - 수행결과 status log 에 로깅하도록
			XLLogger.outputInfoLog("[FINISH JOB][" +  this.jobRunPol.getPolName() + "] totalCommitCnt : " + this.totalCommitCnt);
						
			 
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
