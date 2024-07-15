package xl.init.engine.altibase;

// gssg - 국가정보자원관리원 데이터이관 사업
// gssg - Altibase to Altibase 지원

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Vector;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.init.conn.XLAltibaseConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLOGCons;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLAltibaseRecvThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// 테이블 컬럼의 정보
	private Vector<XLJobColInfo> vtColInfo = null;
	
	private XLAltibaseConnection altibaseConnObj = null;
	private PreparedStatement pstmtSelect = null;
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLAltibaseRecvThread(XLJobRunPol jobRunPol) {
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
			

			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();			
			
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
						
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - Altibase to Altibase 지원
					case XLDicInfoCons.NIBBLE:
					case XLDicInfoCons.BYTE:

						byte[] bAry = rs.getBytes(i+1);
						
						if ( bAry == null ) {
							value = null;
						} else {
							value = XLUtil.bytesToHexString(bAry);
						}
						
						break;

					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - Altibase to Altibase 지원
					case XLDicInfoCons.BLOB:
						
						byte[] dataBytes = null;
						
						if ( XLConf.XL_LOB_STREAM_YN ) {
						
							dataBytes = getReaderBlob(rs.getBinaryStream(i+1));
							
							if ( dataBytes == null ) {
								value = null;
							} else {
								value = XLUtil.bytesToHexString(dataBytes);
							}
						} else {
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
			try { if ( this.altibaseConnObj != null ) this.altibaseConnObj.closeConnection(); } catch (Exception e) {} finally { this.altibaseConnObj = null; }
		}
		
		
		
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
