package xl.init.engine.mysql;


// gssg - xl m2m 최초 포팅 개발 : TODO

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Vector;

import oracle.jdbc.OraclePreparedStatement;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.init.conn.XLMariaDBConnection;
import xl.init.conn.XLMySQLConnection;
import xl.init.conn.XLOracleConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLOGCons;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLMySQLRecvThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
	
	private XLMySQLConnection mySQLConnObj = null;
	private PreparedStatement pstmtSelect = null;
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLMySQLRecvThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		
		// 대상 테이블의 컬럼 정보
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][RECV]";
	}

	@Override
	public void run(){
		
		ResultSet rs = null;
		
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting RecvThread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			// Oracle Connection 생성
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();
			
			
			// gssg - xl m2m 최초 포팅 개발 - TODO
			// gssg - xl m2m 기능 추가  - 0413
			mySQLConnObj = new XLMySQLConnection(
					sdbInfo.getIp(), 
					sdbInfo.getDbSid(),
					sdbInfo.getUserId(),
					sdbInfo.getPasswd(),
					sdbInfo.getPort(), 
					sdbInfo.getDbType()
					);
			
			// Target DB Connection
			// gssg - xl m2m 기능 추가  - 0413
			if ( !mySQLConnObj.makeConnection() ) {
				
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

			// gssg - xl m2m srcSelect check
			// XLLogger.outputInfoLog("GSSG DEBUG getSrcSelectSQL = " + this.jobRunPol.getSrcSelectSql());
			
			// Target 반영 insert preparedStatement 구문 생성
			
			// gssg - xl m2m 기능 추가  - 0413
			this.pstmtSelect = (PreparedStatement)mySQLConnObj.getConnection().prepareStatement(this.jobRunPol.getSrcSelectSql());
			
			
			// gssg - xl m2m 기능 추가  - 0413
			this.pstmtSelect.setFetchSize(XLConf.XL_FETCH_SIZE);
			
			stime = System.currentTimeMillis();
			
			rs = this.pstmtSelect.executeQuery();
			
			Vector<ArrayList<String>> vtData = new Vector<ArrayList<String>>();
			
			String value = null;
			
			
			long rowCnt = 0;
			
			long recvCnt = 0; // skip 한것을 제외한 select count 건수
			
			while ( rs.next() ) {
			
				rowCnt++;
				// 이미 commit된 건수는 skip
				if ( rowCnt <= this.jobRunPol.getCondCommitCnt() ) {
					continue;
				}
				
				ArrayList<String> arrayList = new ArrayList<>(); // record Data			
				for (int i=0; i<this.vtColInfo.size(); i++) {
					
					XLJobColInfo colInfo = this.vtColInfo.get(i);
					
					switch ( colInfo.getDataType() ) {
										
					// gssg - xl MariaDB 동기화					
					case XLDicInfoCons.YEAR:
//						this.pstmtInsert.setDate(setIdx, Date.valueOf(value));
						
						value = rs.getString(i+1);
						if ( value == null ) {
							value = null;
						} else {
							SimpleDateFormat transFormat = new SimpleDateFormat("yyyy");					
							java.util.Date year = transFormat.parse(value);							
							value = transFormat.format(year);	
						}
																			
						break;
						
						
					// gssg - xl MariaDB 동기화
					// gssg - xl o2m 보완
					// gssg - o2m 하다가 m2m bulk mode 보완
					case XLDicInfoCons.BIT:
					case XLDicInfoCons.BINARY:
					case XLDicInfoCons.VARBINARY:

						byte[] bAry = rs.getBytes(i+1);
						
						if ( bAry == null ) {
							value = null;
						} else {
							value = XLUtil.bytesToHexString(bAry);
						}
						
						break;

						// gssg - xl m2m 기능 추가  - 0413						
						// gssg - xl MariaDB 동기화
						case XLDicInfoCons.TEXT:
						case XLDicInfoCons.MEDIUMTEXT:
						case XLDicInfoCons.LONGTEXT:
						
						if ( XLConf.XL_LOB_STREAM_YN ) { // cksohn - xl XL_LOB_STREAM_YN=Y|*N
							
							value = getReaderClob(rs.getCharacterStream(i+1));
							
						} else {
							value = rs.getString(i+1);
						}
						break;
						
						// gssg - xl m2m 기능 추가  - 0413
						// gssg - xl MariaDB 동기화
						case XLDicInfoCons.TINYBLOB:
						case XLDicInfoCons.BLOB:
						case XLDicInfoCons.MEDIUMBLOB:
						case XLDicInfoCons.LONGBLOB:
						
						byte[] dataBytes = null;
						
						if ( XLConf.XL_LOB_STREAM_YN ) { // cksohn - xl XL_LOB_STREAM_YN=Y|*N
						
							dataBytes = getReaderBlob(rs.getBinaryStream(i+1));
							
							if ( dataBytes == null ) {
								value = null;
							} else {
								value = XLUtil.bytesToHexString(dataBytes);
							}
						} else {
							// krs.getBytes(i+1)
							dataBytes = rs.getBytes(i+1);
							
							if ( dataBytes == null ) {
								value = null;
							} else {
								value = XLUtil.bytesToHexString(dataBytes);
							}

						}
						break;
						
					default : 
						value = rs.getString(i+1);
						break;
						
					}	
					arrayList.add(value);					
				} // for-end 1개의 record 추출 종료
				
				vtData.add(arrayList);
				
				recvCnt++;
				
				if ( vtData.size() >= XLConf.XL_MGR_SEND_COUNT ) {
					// dataQ로 전달
					Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
					
					while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // 내부 Q size check
						XLLogger.outputInfoLog("[RUN] INTERNAL_QSIZE : " + this.dataQ.size());
						try { Thread.sleep(1000); } catch (Exception ee) {return;}						
					}
					
					this.dataQ.addDataQ(vtDataSend);
					
					vtData = new Vector<ArrayList<String>>(); // clear Vector
				}
				
			} // while-end
			
			// 남아 있는 데이터 Send
			if ( vtData.size() > 0 ) {
				// dataQ로 전달
				Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
				
				while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // 내부 Q size check
					XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
					try { Thread.sleep(1000); } catch (Exception ee) {return;}						
				}
				
				this.dataQ.addDataQ(vtDataSend);
				
				vtData = new Vector<ArrayList<String>>(); // clear Vector
			}
			
			// 정상 종료시 EOF 데이터 전송
			//ArrayList<String> arrayListEOF = new ArrayList<>(); // record Data	
			//arrayListEOF.add(null);
			//vtData.add(arrayListEOF);
			vtData.add(null);
			Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
			while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // 내부 Q size check
				XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
				try { Thread.sleep(1000); } catch (Exception ee) {return;}						
			}
			
			this.dataQ.addDataQ(vtDataSend);
			
			etime = System.currentTimeMillis();
			
			long elapsedTime = (etime-stime) / 1000;
			
			//XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Completed Job Recv : " + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("\tTotal Select count : " + recvCnt + " / Elapsed time(sec) : " +  elapsedTime);
			//XLLogger.outputInfoLog("");
			
			return;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {
			
			try { if ( this.pstmtSelect != null ) this.pstmtSelect.close(); } catch (Exception e) {} finally { this.pstmtSelect = null; }
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }
			// gssg - xl m2m 기능 추가  - 0413
			try { if ( this.mySQLConnObj != null ) this.mySQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.mySQLConnObj = null; }
		}
		
		
		
	}
	
	// cksohn - xlim LOB 타입 지원
	private String getReaderClob( Reader _reader ) {
        
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(_reader, XLConf.XL_APPLY_LOB_STREAM_BUFFERSIZE);

//            String line;
//            while(null != (line = br.readLine())) {
//                sb.append(line);
//            }
            // cksohn - CLOB read 방식 오류 수정 start - [
            String line;
            char[] buffer = new char[XLConf.XL_APPLY_LOB_STREAM_BUFFERSIZE];
            int readCnt = 0;
            while( (readCnt = br.read(buffer)) > 0 ) {

          	  char[] readBuf = new char[readCnt];        	  
          	  System.arraycopy(buffer, 0, readBuf, 0, readCnt);
          	  sb.append( new String(readBuf) );
          	  
            }
            // ] - end cksohn - CLOB read 방식 오류 수정
            
            br.close();
        } catch (Exception e) {
            // handle this exception
        } finally {
//            try{ _reader.close(); } catch (Exception e) {}
        }
        return sb.toString();
     
    }
	
	// cksohn - xlim LOB 타입 지원
    private  byte[] getReaderBlob( InputStream _reader ) {
        
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
//	            byte[] buffer = new byte[65536];
            byte[] buffer = new byte[XLConf.XL_APPLY_LOB_STREAM_BUFFERSIZE];
            int readcount = 0;
            // 파일로부터 읽어들인 바이트 배열을 ByteArrayOutputStream으로 출력한다.
            while((readcount = _reader.read(buffer)) != -1){
                baos.write(buffer, 0, readcount);     
            }
            // ByteArrayOutputStream의 내부공간에 저장된
            return baos.toByteArray();
//	            
        } catch (Exception e) {
            // handle this exception
            return null;
        } finally {
            try{ baos.close(); } catch (Exception e) {}
//            try{ _reader.close(); } catch (Exception e) {}
	    
        } 
    }
}
