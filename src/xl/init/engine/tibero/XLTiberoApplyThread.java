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
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
		
	private XLTiberoConnection tiberoConnObj = null;
	private PreparedStatement pstmtInsert = null;
	
	private Connection cataConn = null;
	// ayzn - XLInit 기능 개발  - DB 엔진 수정 :: jobq, cond commit 주석
	//private PreparedStatement pstmtUpdateJobQCommitCnt = null;
	//private PreparedStatement pstmtUpdateCondCommitCnt = null;
	
	
	private long applyCnt = 0;
	private long totalCommitCnt =0; // 기존 수행에서 commit된 건수(condCommitCnt) + 이번 수행에서 commit된 건수(applyCnt)
	
	long totalApplyCnt = 0; // 이번 작업수행시 실제로 insert를 수행한 건수 (누적건수 아님)
	
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLTiberoApplyThread(XLJobRunPol jobRunPol) {
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
			// ayzn - XLInit 기능 개발  - DB 엔진 수정: jobq, cond commit 주석
			//this.pstmtUpdateJobQCommitCnt = mDBMgr.getPstmtUpdateJobQCommitCnt(this.cataConn);
			//this.pstmtUpdateCondCommitCnt = mDBMgr.getPstmtUpdateCondCommitCnt(this.cataConn);
			
			
			// Oracle Connection 생성
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
					// Recv Thread의 상태는 Recv Thread의 비정상 종료시 메시지를 setting 하도록 되어 있는데, 
					// 이를 가지고 check 하자. 
					if ( this.jobRunPol.getErrMsg_Recv() != null ) {
						// RECV Thread 비정상 종료시
						
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
				
				//Queue에서 Apply할 Object를 가지고 온다. 
				vtData = dataQ.getDataQ();
				
				//Queue에서 삭제한다.
				dataQ.removeDataQ();
				
				
				// XLLogger.outputInfoLog("CKSOHN DEBUG  vtData.size ---> " + vtData.size());
				// EOF check
				if ( vtData.size() == 1) {
					
					
					ArrayList<String> recordDataArray = vtData.get(0);
					
					if ( recordDataArray == null ) {
						
						this.pstmtInsert.executeBatch(); // final sendBatch call
						this.tiberoConnObj.commit();
						
						// 누적 commit 건수
						this.totalCommitCnt += this.applyCnt;
						this.totalApplyCnt += this.applyCnt;
						
						XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt + " / " + this.jobRunPol.getPolName());
						this.applyCnt = 0; // 초기화
						// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
						/*
						 * this.pstmtUpdateJobQCommitCnt.setLong(1, this.totalCommitCnt);
						 * this.pstmtUpdateJobQCommitCnt.setLong(2, this.jobRunPol.getJobseq());
						 * this.pstmtUpdateJobQCommitCnt.setString(3, this.jobRunPol.getPolName());
						 * this.pstmtUpdateJobQCommitCnt.executeUpdate();
						 * 
						 * this.pstmtUpdateCondCommitCnt.setLong(1, this.totalCommitCnt);
						 * this.pstmtUpdateCondCommitCnt.setString(2, this.jobRunPol.getPolName());
						 * this.pstmtUpdateCondCommitCnt.setLong(3, this.jobRunPol.getCondSeq()); //
						 * gssg - 일본 네트워크 분산 처리 this.pstmtUpdateCondCommitCnt.setLong(4,
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
				
				// 데이터 반영
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
			
			// 뒷처리
			this.jobRunPol.setErrMsg_Apply(errMsg);
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			FinishJob();
			
		} finally {
			
			try { if ( this.pstmtInsert != null ) this.pstmtInsert.close(); } catch (Exception e) {} finally { this.pstmtInsert = null; }
			try { if ( this.tiberoConnObj != null ) this.tiberoConnObj.closeConnection(); } catch (Exception e) {} finally { this.tiberoConnObj = null; }
			
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석
			//try { if ( this.pstmtUpdateJobQCommitCnt != null ) this.pstmtUpdateJobQCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateJobQCommitCnt = null; }
			//try { if ( this.pstmtUpdateCondCommitCnt != null ) this.pstmtUpdateCondCommitCnt.close(); } catch (Exception e) {} finally { this.pstmtUpdateCondCommitCnt = null; }

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
			// ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobseq 제외
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
						// 데이터 타입별 반영
						XLJobColInfo colInfo = this.vtColInfo.get(j);
						
						switch ( colInfo.getDataType() ) {
						
//						// binary 타입 (hex-->binary)
//						case XLDicInfoCons.RAW:
//						
//							byte[] bAry = XLUtil.hexToByteArray(value);
//							this.pstmtInsert.setBytes(setIdx, bAry);
//							break;
							
						// LONG/LONGRAW 및 LOB 타입은 나중에 처리....
						
						// cksohn - xlim LOB 타입 지원 start - [
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.NCLOB:
						case XLDicInfoCons.LONG:
						case XLDicInfoCons.XMLTYPE:
							this.pstmtInsert.setString(setIdx, value);
							break;
							
						case XLDicInfoCons.BLOB:
						case XLDicInfoCons.LONGRAW:
						// gssg - xl p2t 지원
						// gssg - p2t normal mode 타입 처리
						case XLDicInfoCons.BYTEA:
						// gssg - 국가정보자원관리원 데이터이관 사업
						// gssg - MySQL to Tibero 지원
						case XLDicInfoCons.TINYBLOB:
						case XLDicInfoCons.MEDIUMBLOB:
						case XLDicInfoCons.LONGBLOB:																			
							byte[] dataBytes = XLUtil.hexToByteArray(value);
							this.pstmtInsert.setBytes(setIdx, dataBytes);
							break;
						// - end cksohn - xlim LOB 타입 지원
							
						case XLDicInfoCons.DATE:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}
							
							// gssg - xl p2t 지원
							// gssg - p2t normal mode 타입 처리
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
								this.pstmtInsert.setString(setIdx, value);
							} else {
								// .0 제거
								value = XLUtil.removeDot(value);
								this.pstmtInsert.setString(setIdx, value);								
							}							
							break;
						
						// gssg - 국가정보자원관리원 데이터이관 사업
						// gssg - Tibero to Tibero 타임존 처리
						case XLDicInfoCons.TIMESTAMP_TZ:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							
							value = XLUtil.removeOracleTZ(value);
							
							this.pstmtInsert.setString(setIdx, value);
							break;

						// gssg - 국가정보자원관리원 데이터이관 사업
						// gssg - Tibero to Tibero 타임존 처리
						case XLDicInfoCons.TIMESTAMP_LTZ:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

								
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.ORACLE ) {
								value = XLUtil.removeOracleTZ(value);
							}
								
							this.pstmtInsert.setString(setIdx, value);
							break;
							
							
							
						// gssg - xl p2t 지원
						// gssg - p2t normal mode 타입 처리
						case XLDicInfoCons.INTERVAL_YM:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

														
							// INTERVAL_YM 처리
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
								// PPAS와 Tibero의 YM INTERVAL 형식의 차이로 인한 에러 사항 보완
								// PPAS YM 형식 : 12 years 1 mon
								// Tibero YM 형식 : +12-01
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
							
							// gssg - xl p2t 지원
							// gssg - p2t normal mode 타입 처리
						case XLDicInfoCons.INTERVAL_DS:
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

							
							// INTERVAL_DS 처리
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리
							if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
							// PPAS와 Tibero의 YM INTERVAL 형식의 차이로 인한 에러 사항 보완
							// PPAS DS 형식 : 50 days 10:20:20.123
							// Tibero DS 형식 : +50 10:20:20.123000
							// 주의사항 - ppas 에서는 day의 값으로 100이상의 값이 들어갈 수 있지만 티베로는 0~99까지만 가능
							String day;
							String second;							
									
							if (value.indexOf(' ') == -1) { // ppas 에서 day 값이 0인 경우 '10:20:20.123' 와 같은 시간만 있는 형식의 string이 넘어옴
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
							
							// gssg - 세븐일레븐 O2MS
							if ( XLConf.XL_MGR_DATA_DEBUG_YN ) {
								XLLogger.outputInfoLog("set " + setIdx + " / " + value);
							}

														
							this.pstmtInsert.setString(setIdx, value);
							
							break;
							
						}						
					}
				} // for-end (j) - 1개의 record 반영
				
				// gssg - 현대차 대방동 - addBatch 처리
//				this.pstmtInsert.executeUpdate();
				this.pstmtInsert.addBatch();
								
				applyCnt++;
				
				if ( applyCnt % this.jobRunPol.getPolCommitCnt() == 0) {
					
					this.pstmtInsert.executeBatch();
					this.tiberoConnObj.commit();
					
					// 누적 commit 건수
					this.totalCommitCnt += this.applyCnt;
					this.totalApplyCnt += this.applyCnt;
					XLLogger.outputInfoLog(this.logHead + " Apply Count : " + this.applyCnt);
					
					this.applyCnt = 0; // 초기화
					
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
	
	
	// JOB 종료
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
