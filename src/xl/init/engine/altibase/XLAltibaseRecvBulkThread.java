package xl.init.engine.altibase;

// gssg - 국가정보자원관리원 데이터이관 사업
// gssg - Altibase to Oracle 지원

import java.io.RandomAccessFile;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Vector;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLAltibaseConnection;
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
 * @author gssg
 * 
 * gssg - BULK mode
 *
 */

public class XLAltibaseRecvBulkThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
		
	private XLAltibaseConnection altibaseConnObj = null;	
	private PreparedStatement pstmtSelect = null;
	
	private RandomAccessFile pipe = null;
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLAltibaseRecvBulkThread(XLJobRunPol jobRunPol) {
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
			
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();			
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo(); 
			byte tdbType = tdbInfo.getDbType();
			
			altibaseConnObj = new XLAltibaseConnection(
					sdbInfo.getIp(), 
					sdbInfo.getDbSid(),
					sdbInfo.getUserId(),
					sdbInfo.getPasswd(),
					sdbInfo.getPort(),
					sdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			if ( !altibaseConnObj.makeConnection() ) {
				
				errMsg = "[EXCEPTION] Failed to make source db connection - " + sdbInfo.getIp() + "/" + sdbInfo.getDbSid();
				XLLogger.outputInfoLog(errMsg);
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead +  "[EXCEPTION] Recv Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Recv(errMsg);
				return;
			} else {
				XLLogger.outputInfoLog(this.logHead + " Source DBMS is connected - " +  sdbInfo.getIp() + "/" + sdbInfo.getDbSid());
			}

			// Target 반영 insert preparedStatement 구문 생성			
			this.pstmtSelect = (PreparedStatement)altibaseConnObj.getConnection().prepareStatement(this.jobRunPol.getSrcSelectSql());
									
			stime = System.currentTimeMillis();
			
			this.pstmtSelect.setFetchSize(XLConf.XL_FETCH_SIZE);
			
			rs = this.pstmtSelect.executeQuery();
						
			String value = null;
			
			byte[] bAry = null;
			
			this.pipe = new RandomAccessFile( this.jobRunPol.getBulk_pipePath(), "rw" );	
			
			long rowCnt = 0;
			
			long recvCnt = 0; // skip 한것을 제외한 select count 건수
			
			StringBuffer sb_record = new StringBuffer(); // record Data

			while ( rs.next() ) {
			
				rowCnt++;
				// 이미 commit된 건수는 skip
				if ( rowCnt <= this.jobRunPol.getCondCommitCnt() ) {
					continue;
				}

				
				for (int i=0; i<this.vtColInfo.size(); i++) {
					
					XLJobColInfo colInfo = this.vtColInfo.get(i);
					switch ( colInfo.getDataType() ) {
												
					case XLDicInfoCons.CHAR:
					case XLDicInfoCons.NCHAR:
					case XLDicInfoCons.VARCHAR:
					case XLDicInfoCons.NVARCHAR:

						value = rs.getString(i+1);						

						if( tdbType == XLCons.ORACLE ) {
							value = XLUtil.replaceCharToCSV_ORACLE(value);							
						} 
						break;
						
					case XLDicInfoCons.CLOB:
						value = rs.getString(i+1);

						if ( value == null ) {
							if( tdbType == XLCons.ORACLE )
								value = "";
						}						
						break;
					
					case XLDicInfoCons.BLOB:
												
							bAry = rs.getBytes(i+1);
							
							if ( bAry != null && tdbType == XLCons.ORACLE ) { // 오라클은 바이너리 지원 안됨
								value = null;
							}
													
						break;												

						
					default : 
												
						value = rs.getString(i+1);
						
						if ( value == null ) {
							if ( tdbType == XLCons.ORACLE )
								value = ""; // oracle은 null 을 "" 로 처리해야 함
						}
												
						break;
						
					}	

					if ( i != 0 ) {
						sb_record.append(",");
					}
					
					sb_record.append("\"").append(value).append("\"");
								
				}
				
				if ( tdbType == XLCons.ORACLE && !XLConf.XL_BULK_ORACLE_EOL.equals("") ) {					
					sb_record.append(XLConf.XL_BULK_ORACLE_EOL);
				} else {
					// 타겟이 오라클일경우는 \n write 안함
					sb_record.append("\n");
				}
								
				recvCnt++;
				
				if ( (recvCnt % XLConf.XL_BATCH_SIZE ) == 0) {
										
					pipe.write(sb_record.toString().getBytes("UTF-8"));

					sb_record = new StringBuffer(); // 초기화
														
				}
				
				
				
			} // while-end
			
			// remain data 처리 
			if ( sb_record.toString().length() > 0 ) {
				pipe.write(sb_record.toString().getBytes("UTF-8"));
				
				// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());
			}
						

			etime = System.currentTimeMillis();
			
			long elapsedTime = (etime-stime) / 1000;
			
			XLLogger.outputInfoLog(this.logHead + " Completed Job Recv : " + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("\tTotal Select count : " + recvCnt + " / Elapsed time(sec) : " +  elapsedTime);
			
			this.jobRunPol.setApplyCnt(recvCnt); // xl recvCnt를 applyCnt로 적용

			this.dataQ.notifyEvent();
			
			return;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {

			try { if (pipe != null) pipe.close(); } catch (Exception e1) {} finally { pipe = null; }			
			
			try { if ( this.pstmtSelect != null ) this.pstmtSelect.close(); } catch (Exception e) {} finally { this.pstmtSelect = null; }
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }
			
			try { if ( this.altibaseConnObj != null ) this.altibaseConnObj.closeConnection(); } catch (Exception e) {} finally { this.altibaseConnObj = null; }
			
			this.dataQ.notifyEvent();
		}
		
		
		
	}
}
