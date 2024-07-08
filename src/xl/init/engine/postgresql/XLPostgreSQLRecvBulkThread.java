package xl.init.engine.postgresql;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Vector;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLMariaDBConnection;
import xl.init.conn.XLPPASConnection;
import xl.init.conn.XLPostgreSQLConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLOGCons;
import xl.init.util.XLException;
import xl.init.util.XLUtil;


/**
 * 
 * @author cksohn
 * 
 * cksohn - BULK mode oracle sqlldr
 *
 */

public class XLPostgreSQLRecvBulkThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;

	// gssg - t2p 보완
	// gssg - postgreSQL 커넥터 수정
	private XLPostgreSQLConnection postgreSQLConnObj = null;
	private PreparedStatement pstmtSelect = null;
	
	
	// cksohn - BULK mode oracle sqlldr
	private RandomAccessFile pipe = null;
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLPostgreSQLRecvBulkThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		
		// 대상 테이블의 컬럼 정보
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][RECV BULK]";
	}

	@Override
	public void run(){
		
		ResultSet rs = null;
		
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting RecvThread(Direct Patn Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			// Oracle Connection 생성
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();
			
			// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL start - [
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo(); 
			byte tdbType = tdbInfo.getDbType();
			// ] - end 
			
			// gssg - xl m2m bulk mode 지원
			postgreSQLConnObj = new XLPostgreSQLConnection(
					sdbInfo.getIp(), 
					sdbInfo.getDbSid(),
					sdbInfo.getUserId(),
					sdbInfo.getPasswd(),
					sdbInfo.getPort(),
					sdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			// gssg - xl m2m bulk mode 지원
			if ( !postgreSQLConnObj.makeConnection() ) {
				
				errMsg = "[EXCEPTION] Failed to make source db connection - " + sdbInfo.getIp() + "/" + sdbInfo.getDbSid();
				XLLogger.outputInfoLog(errMsg);
				// TODO 여기서 Catalog DB에 실패로 update 치고 끝나야 하는데,, catalog 가 타겟에 있을 경우 문제가 되긴함. 
				//      그러면, 추후 수행시 깨끗하게 JOBQ를 지우고 수행하도록 조치해야 할 수도 있음. 
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead +  "[EXCEPTION] Recv Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Recv(errMsg);
				return;
			} else {
				XLLogger.outputInfoLog(this.logHead + " Source DBMS is connected - " +  sdbInfo.getIp() + "/" + sdbInfo.getDbSid());
			}

			// Target 반영 insert preparedStatement 구문 생성
			
			// gssg - xl m2m bulk mode 지원
			this.pstmtSelect = (PreparedStatement)postgreSQLConnObj.getConnection().prepareStatement(this.jobRunPol.getSrcSelectSql());
			
						
			stime = System.currentTimeMillis();
			
			this.pstmtSelect.setFetchSize(XLConf.XL_FETCH_SIZE);

			rs = this.pstmtSelect.executeQuery();
			
			Vector<ArrayList<String>> vtData = new Vector<ArrayList<String>>();
			
			String value = null;
			
			// gssg - xl p2p 지원
			byte[] bAry = null;
			
			// cksohn - BULK mode oracle sqlldr
			this.pipe = new RandomAccessFile( this.jobRunPol.getBulk_pipePath(), "rw" );
			
			
			// cksohn - BULK mode oracle sqlldr
			// TEST CODE!!!
			// FileWriter fw_test = new FileWriter("/tmp/ck.dat", false);
			
			
			long rowCnt = 0;
			
			long recvCnt = 0; // skip 한것을 제외한 select count 건수
			
			// cksohn - BULK mode oracle sqlldr
			StringBuffer sb_record = new StringBuffer(); // record Data
			
			while ( rs.next() ) {
			
				rowCnt++;
				// 이미 commit된 건수는 skip
				if ( rowCnt <= this.jobRunPol.getCondCommitCnt() ) {
					continue;
				}
				
				
				// ArrayList<String> arrayList = new ArrayList<>(); // record Data

				
				for (int i=0; i<this.vtColInfo.size(); i++) {
					
					XLJobColInfo colInfo = this.vtColInfo.get(i);
					switch ( colInfo.getDataType() ) { // gssg - xl m2m bulk mode 지원

					
					// gssg - xl p2p 지원
					// gssg - p2p bulk mode 타입 처리
					// gssg - o2m 하다가 p2p bulk mode 보완
					 case XLDicInfoCons.CHAR:
					 case XLDicInfoCons.VARCHAR:					
					 case XLDicInfoCons.VARCHAR2:					
						
						 value = rs.getString(i+1);
						 
						 // gssg - xl p2t 지원
						 // gssg - p2t bulk mode 타입 처리
						 // gssg - xl 전체적으로 보완2
						 // gssg - PostgreSQL 커넥터 처리
						 if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) {
							value = XLUtil.replaceCharToCSV_PPAS(value);							 
						 } else if ( tdbType == XLCons.TIBERO ) {
							 value = XLUtil.replaceCharToCSV_TIBERO(value);
						 }

						break;
						
					// gssg - xl p2p 지원
					// gssg - p2p bulk mode 타입 처리
					 case XLDicInfoCons.TEXT:
					 case XLDicInfoCons.XML:
						 							
						 	value = rs.getString(i+1);				
						 
							value = XLUtil.replaceCharToCSV_PPAS(value);
							
							break;

					// gssg - xl p2p 지원
					// gssg - p2p bulk mode 타입 처리
					 case XLDicInfoCons.BYTEA:
						 
							bAry = rs.getBytes(i+1);
						 
							if (bAry == null) { // gssg - ppas bulk mode null 처리
								value = "\\N";
							} else {
								value = "\\x" + XLUtil.bytesToHexString(bAry);	//  ppas \x 붙여줌
							}
							break;


						
					default : 
						value = rs.getString(i+1);
						
						 // gssg - xl p2t 지원
						 // gssg - p2t bulk mode 타입 처리
						 // gssg - xl 전체적으로 보완2
						 // gssg - PostgreSQL 커넥터 처리
						if ( value == null ) {							
							// gssg - 모듈 보완
							// gssg - P2P - \N값 처리
//							if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL )
//								value = "\\N";
							if ( tdbType == XLCons.TIBERO )
								value = "";							
						}
						break;
						
					}	
					// arrayList.add(value);
					// cksohn - BULK mode oracle sqlldr
					if ( i != 0 ) {
						sb_record.append(",");
						
					}
					
					// gssg - 전체적으로 보완_start_20221101
					// gssg - potgresql to postgresql bulk mode 널값 처리
					// gssg - 모듈 보완
					// gssg - P2P - \N값 처리
//					if ( !value.equals("\\N") ) {
//						sb_record.append("\"").append(value).append("\"");
//					} else if ( tdbType != XLCons.PPAS && tdbType != XLCons.POSTGRESQL ) {
//						sb_record.append("\"").append(value).append("\"");
//					} else {
//						sb_record.append(value);
//					}
					if ( (tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL) && value == null ) {
						sb_record.append("\\N");
					} else  {
						sb_record.append("\"").append(value).append("\"");
					} 
					
					
				} // for-end 1개의 record 추출 종료
				
				// gssg - xl m2m bulk mode 지원
				if ( tdbType == XLCons.ORACLE && !XLConf.XL_BULK_ORACLE_EOL.equals("")) {
					
					sb_record.append(XLConf.XL_BULK_ORACLE_EOL);
				} else {
					// cksohn - xl bulk mode for oracle - 결과 처리 오류 수정
					// 타겟이 오라클일경우는 \n write 안함
					sb_record.append("\n");
				}
				
				
//				XLLogger.outputInfoLog("CKSOHN DEBUG sb_record = " + sb_record.toString());
				
				
				recvCnt++;
				
				
				// cksohn - BULK mode oracle sqlldr
				// if ( (recvCnt % XLConf.XL_MGR_SEND_COUNT ) == 0) {
				// cksohn - xl bulk mode 성능 개선 - t2o
				if ( (recvCnt % XLConf.XL_BATCH_SIZE ) == 0) {
					
		
					// pipe.writeBytes(sb_record.toString());
					// cksohn - xl bulk mode for oracle - special character loading error
					pipe.write(sb_record.toString().getBytes("UTF-8"));
										
					// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());

					sb_record = new StringBuffer(); // 초기화
					
					
					
				}
				
				
				
			} // while-end
			
			// remain data 처리 
			if ( sb_record.toString().length() > 0 ) {
				// 나머지 데이터 존재 
				// pipe.writeBytes(sb_record.toString());
				// cksohn - xl bulk mode for oracle - special character loading error
				pipe.write(sb_record.toString().getBytes("UTF-8"));
				
				// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());
			}
			
			// TEST CODE
			//fw_test.close();
			
			// cksohn - BULK mode oracle sqlldr - comment
			// 남아 있는 데이터 Send
//			if ( vtData.size() > 0 ) {
//				// dataQ로 전달
//				Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
//				
//				while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // 내부 Q size check
//					XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
//					try { Thread.sleep(1000); } catch (Exception ee) {return;}						
//				}
//				
//				this.dataQ.addDataQ(vtDataSend);
//				
//				vtData = new Vector<ArrayList<String>>(); // clear Vector
//			}
//			
//			// 정상 종료시 EOF 데이터 전송
//			//ArrayList<String> arrayListEOF = new ArrayList<>(); // record Data	
//			//arrayListEOF.add(null);
//			//vtData.add(arrayListEOF);
//			vtData.add(null);
//			Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
//			while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // 내부 Q size check
//				XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
//				try { Thread.sleep(1000); } catch (Exception ee) {return;}						
//			}
//			
//			this.dataQ.addDataQ(vtDataSend);
			
			// gssg - xl p2t 지원
			// gssg - p2t bulk mode 스레드 순서 조정
			this.jobRunPol.setWrite(true);

			etime = System.currentTimeMillis();
			
			long elapsedTime = (etime-stime) / 1000;
			
			//XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Completed Job Recv : " + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("\tTotal Select count : " + recvCnt + " / Elapsed time(sec) : " +  elapsedTime);
			//XLLogger.outputInfoLog("");
			
			// cksohn - BULK mode oracle sqlldr
			this.dataQ.notifyEvent();
			
			return;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {
			
			// gssg - xl p2t 지원
			// gssg - p2t bulk mode 스레드 순서 조정
			if(this.jobRunPol.getTdbInfo().getDbType() == XLCons.TIBERO) {				

				int chkCnt = 0;

				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] ----- START isLoadQuery WHILE!!!!! - " + this.jobRunPol.isLoadQuery());
				}
					try {
						while ( !this.jobRunPol.isLoadQuery() && chkCnt <= 10 ) {

						chkCnt++;
						XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][RECV BULK] Waiting Check Loader State.(" + chkCnt + ")");					
						Thread.sleep(1000);
						}					
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] ----- END isLoadQuery WHILE!!!! - " + this.jobRunPol.isLoadQuery());
				}	
				
			}

			try { if (pipe != null) pipe.close(); } catch (Exception e1) {} finally { pipe = null; }
			
			try { if ( this.pstmtSelect != null ) this.pstmtSelect.close(); } catch (Exception e) {} finally { this.pstmtSelect = null; }
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }

			// gssg - xl 전체적으로 보완
		    // gssg - m2m bulk mode thread 순서 조정
			// gssg - xl p2p 지원
			// gssg - xl p2t 지원
			// gssg - p2t bulk mode 스레드 순서 조정
			// gssg - xl 전체적으로 보완2
			// gssg - PostgreSQL 커넥터 처리
			// gssg - 모듈 보완
			// gssg - P2P - bulk 스레드 순서 조정
//			if ( this.jobRunPol.getTdbInfo().getDbType() == XLCons.PPAS || 
//					this.jobRunPol.getTdbInfo().getDbType() == XLCons.POSTGRESQL ) {				
//				int chkCnt = 0;
//
//				if ( XLConf.XL_MGR_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] ----- START isLoadQuery WHILE!!!!! - " + this.jobRunPol.isLoadQuery());
//				}
//
//					try {
//						while ( !this.jobRunPol.isLoadQuery() && chkCnt <= 10 ) {
//
//						chkCnt++;
//						XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][RECV BULK] Waiting Check Loader State.(" + chkCnt + ")");					
//						Thread.sleep(1000);
//						}
//						
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				if ( XLConf.XL_MGR_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] ----- END isLoadQuery WHILE!!!! - " + this.jobRunPol.isLoadQuery());
//				}				
//			}
			
			try { if ( this.postgreSQLConnObj != null && this.jobRunPol.isLoadQuery() ) this.postgreSQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.postgreSQLConnObj = null; }

//			try { if ( this.ppasConnObj != null ) this.ppasConnObj.closeConnection(); } catch (Exception e) {} finally { this.ppasConnObj = null; }
			
			// cksohn - BULK mode oracle sqlldr
			this.dataQ.notifyEvent();
		}
		
		
		
	}
}
