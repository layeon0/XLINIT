package xl.init.engine.oracle;

import java.io.BufferedReader;



import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Vector;

import oracle.jdbc.OraclePreparedStatement;

// gssg - o2o damo ����
import com.penta.scpdb.*;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLOracleConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLOGCons;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLOracleRecvThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
	private XLOracleConnection oraConnObj = null;
	private OraclePreparedStatement pstmtSelect = null;
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLOracleRecvThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		
		// ��� ���̺��� �÷� ����
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][RECV]";
	}

	@Override
	public void run(){
		
		ResultSet rs = null;
		
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting RecvThread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			// Oracle Connection ����
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();
			
			// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
			// gssg - damo api ����
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			oraConnObj = new XLOracleConnection(
					sdbInfo.getIp(), 
					sdbInfo.getDbSid(),
					sdbInfo.getUserId(),
					sdbInfo.getPasswd(),
					sdbInfo.getPort(),
					sdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			if ( !oraConnObj.makeConnection() ) {
				
				errMsg = "[EXCEPTION] Failed to make source db connection - " + sdbInfo.getIp() + "/" + sdbInfo.getDbSid();
				XLLogger.outputInfoLog(errMsg);
				// TODO ���⼭ Catalog DB�� ���з� update ġ�� ������ �ϴµ�,, catalog �� Ÿ�ٿ� ���� ��� ������ �Ǳ���. 
				//      �׷���, ���� ����� �����ϰ� JOBQ�� ����� �����ϵ��� ��ġ�ؾ� �� ���� ����. 
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead +  "[EXCEPTION] Recv Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Recv(errMsg);
				return;
			} else {
				XLLogger.outputInfoLog(this.logHead + " Source DBMS is connected - " +  sdbInfo.getIp() + "/" + sdbInfo.getDbSid());
			}

			// Target �ݿ� insert preparedStatement ���� ����
			
			this.pstmtSelect = (OraclePreparedStatement)oraConnObj.getConnection().prepareStatement(this.jobRunPol.getSrcSelectSql());
			
			
			
			stime = System.currentTimeMillis();
			
			rs = this.pstmtSelect.executeQuery();
			
			Vector<ArrayList<String>> vtData = new Vector<ArrayList<String>>();
			
			String value = null;
			
			
			long rowCnt = 0;
			
			long recvCnt = 0; // skip �Ѱ��� ������ select count �Ǽ�
						
			while ( rs.next() ) {
			
				rowCnt++;
				// �̹� commit�� �Ǽ��� skip
				if ( rowCnt <= this.jobRunPol.getCondCommitCnt() ) {
					continue;
				}
				
				ArrayList<String> arrayList = new ArrayList<>(); // record Data			
				for (int i=0; i<this.vtColInfo.size(); i++) {
					
					XLJobColInfo colInfo = this.vtColInfo.get(i);
					switch ( colInfo.getDataType() ) {
					
					// gssg - �����Ϸ��� O2MS
					case XLDicInfoCons.RAW:
					
						if ( tdbInfo.getDbType() == XLCons.MSSQL ) {
							byte[] bAry = rs.getBytes(i+1);
							
							if ( bAry == null ) {
								value = null;
							} else {
								value = XLUtil.bytesToHexString(bAry);
							}							
						} else {
							value = rs.getString(i+1);
							if (value != null && value.equals("")) {
								value = null; // oracle�� "" �� null
							}
						}
						
						break;
					
					// cksohn - xlim LOB Ÿ�� ���� start - [
					case XLDicInfoCons.CLOB:
					case XLDicInfoCons.NCLOB:
					case XLDicInfoCons.LONG:
					case XLDicInfoCons.XMLTYPE:
												
						if ( XLConf.XL_LOB_STREAM_YN ) { // cksohn - xl XL_LOB_STREAM_YN=Y|*N
							
							value = getReaderClob(rs.getCharacterStream(i+1));
							if (value != null && value.equals("")) {
								value = null; // oracle�� "" �� null								
							}						
						} else if ( value != null && colInfo.getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
							// gssg - ��� ����
							// gssg - O2P - 0x00 ������ ó��					
							value = rs.getString(i+1);
//							byte[] hexBytesChar = rs.getBytes(i+1);
//							byte[] hexBytesChar = value.getBytes(StandardCharsets.UTF_8);
							byte[] hexBytesChar = null;
						    if ( tdbInfo.getnCharSet().contains("WIN949") || tdbInfo.getnCharSet().contains("CP949")) {
								hexBytesChar = value.getBytes("MS949");
						    } else if ( tdbInfo.getCharSet().contains("EUC") ) {
								hexBytesChar = value.getBytes("EUC-KR");
						    } else if ( tdbInfo.getCharSet().contains("KSC5601") ) {
								hexBytesChar = value.getBytes("KSC5601");
						    } else {
								hexBytesChar = value.getBytes("UTF-8");
						    }												
							value = XLUtil.bytesToHexString(hexBytesChar);							
						} else {
							value = rs.getString(i+1);
						}
						break;
						
					case XLDicInfoCons.BLOB:
					case XLDicInfoCons.LONGRAW:
						
						byte[] dataBytes = null;
						
						if ( XLConf.XL_LOB_STREAM_YN ) { // cksohn - xl XL_LOB_STREAM_YN=Y|*N
						
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
					// ] - end cksohn - xlim LOB Ÿ�� ����
						
					
					case XLDicInfoCons.CHAR:
					case XLDicInfoCons.NCHAR:
												
						value = rs.getString(i+1);
						
						if ( value != null && colInfo.getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
							// gssg - ��� ����
							// gssg - O2P - 0x00 ������ ó��													
//							byte[] hexBytesChar = value.getBytes(StandardCharsets.UTF_8);
							byte[] hexBytesChar =rs.getBytes(i+1);
												
							value = XLUtil.bytesToHexString(hexBytesChar);							
						} 

						
//						if (value != null && value.equals("")) {
//							value = null; // oracle�� "" �� null
//						} else	if ( value != null && XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
//							// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE
//							// Hex String
//							value = new String(XLUtil.hexToByteArray(value), XLConf.XL_SRC_CHAR_ENCODE) ;							
//						}						
												
						break;
						
					// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
					// gssg - char, nchar �и�
					case XLDicInfoCons.VARCHAR2:
					case XLDicInfoCons.VARCHAR:

						value = rs.getString(i+1);

						if ( value != null ) {
							if ( colInfo.getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
								// gssg - ��� ����
								// gssg - O2P - 0x00 ������ ó��
//								byte[] hexBytesChar = value.getBytes(StandardCharsets.UTF_8);
								byte[] hexBytesChar =rs.getBytes(i+1);
													
								value = XLUtil.bytesToHexString(hexBytesChar);							
								
							} else if ( colInfo.getSecYN().equals("Y") ) {

								// gssg - o2o damo ����							
								String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;	
							    ScpDbAgent agt = new ScpDbAgent();						      
							    byte[] enc = null;
//							    XLLogger.outputInfoLog("################################################################################");
//								XLLogger.outputInfoLog("[GSSG DEBUG] VARCHAR enc = agt.ScpEncB64( iniFilePath, \"KEY1\", value.getBytes(\"MS949\") );");
//								XLLogger.outputInfoLog("################################################################################");
							    
							    // gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
							    // gssg - damo api ����							    
							    if ( tdbInfo.getCharSet().contains("WIN949") || tdbInfo.getCharSet().contains("CP949")) {
							    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));							    	
							    } else if ( tdbInfo.getCharSet().contains("EUC") ) {
							    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("EUC-KR"));							    								    	
							    } else {
							    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("UTF-8"));							    								    	
							    }							    							    							    
							    
//							    enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));
							
								value = new String(enc);
								
							}
						}
						
						
						
//						if (value != null && value.equals("")) {
//							value = null; // oracle�� "" �� null
//						} else if ( value != null && colInfo.getSecYN().equals("Y") ) {
//
//							// gssg - o2o damo ����							
//							String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;	
//						    ScpDbAgent agt = new ScpDbAgent();						      
//						    byte[] enc = null;
//
////						    XLLogger.outputInfoLog("################################################################################");
////							XLLogger.outputInfoLog("[GSSG DEBUG] VARCHAR enc = agt.ScpEncB64( iniFilePath, \"KEY1\", value.getBytes(\"MS949\") );");
////							XLLogger.outputInfoLog("################################################################################");
//
//						    enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));
//						
//							value = new String(enc);
//
//						} else if ( value != null && XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
//							
//							// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE							
//							// Hex String
//							value = new String(XLUtil.hexToByteArray(value), XLConf.XL_SRC_CHAR_ENCODE) ;
//						}						
						
						break;
						
						// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
						// gssg - char, nchar �и�
						case XLDicInfoCons.NVARCHAR2:
						case XLDicInfoCons.NVARCHAR:

							value = rs.getString(i+1);

							if ( value != null ) {
								if ( colInfo.getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
									// gssg - ��� ����
									// gssg - O2P - 0x00 ������ ó��
									// gssg - ����� O2O
									// gssg - raw_to_varchar2 ��� ����							 
//									byte[] hexBytesChar = value.getBytes(StandardCharsets.UTF_8);
									byte[] hexBytesChar =rs.getBytes(i+1);
														
									value = XLUtil.bytesToHexString(hexBytesChar);							
									
								} else if ( colInfo.getSecYN().equals("Y") ) {

									// gssg - o2o damo ����							
									String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;	
								    ScpDbAgent agt = new ScpDbAgent();						      
								    byte[] enc = null;
//								    XLLogger.outputInfoLog("################################################################################");
//									XLLogger.outputInfoLog("[GSSG DEBUG] VARCHAR enc = agt.ScpEncB64( iniFilePath, \"KEY1\", value.getBytes(\"MS949\") );");
//									XLLogger.outputInfoLog("################################################################################");

								    // gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
								    // gssg - damo api ����							    
								    if ( tdbInfo.getnCharSet().contains("WIN949") || tdbInfo.getnCharSet().contains("CP949")) {
								    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));							    	
								    } else if ( tdbInfo.getCharSet().contains("EUC") ) {
								    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("EUC-KR"));							    								    	
								    } else {
								    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("UTF-8"));							    								    	
								    }							    							    
								    
//								    enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));
								
									value = new String(enc);
									
								}
							}
							
							
							
//							if (value != null && value.equals("")) {
//								value = null; // oracle�� "" �� null
//							} else if ( value != null && colInfo.getSecYN().equals("Y") ) {
	//
//								// gssg - o2o damo ����							
//								String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;	
//							    ScpDbAgent agt = new ScpDbAgent();						      
//							    byte[] enc = null;
	//
////							    XLLogger.outputInfoLog("################################################################################");
////								XLLogger.outputInfoLog("[GSSG DEBUG] VARCHAR enc = agt.ScpEncB64( iniFilePath, \"KEY1\", value.getBytes(\"MS949\") );");
////								XLLogger.outputInfoLog("################################################################################");
	//
//							    enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));
//							
//								value = new String(enc);
	//
//							} else if ( value != null && XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
//								
//								// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE							
//								// Hex String
//								value = new String(XLUtil.hexToByteArray(value), XLConf.XL_SRC_CHAR_ENCODE) ;
//							}						
							
							break;
												
					default : 
						value = rs.getString(i+1);

						if (value != null && value.equals("")) {
							value = null; // oracle�� "" �� null
						}

						break;
						
					}	
					arrayList.add(value);					
				} // for-end 1���� record ���� ����
								
				vtData.add(arrayList);
								
				recvCnt++;
				
				if ( vtData.size() >= XLConf.XL_MGR_SEND_COUNT ) {
					// dataQ�� ����
					Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
					
					while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // ���� Q size check
						//XLLogger.outputInfoLog("[RUN] INTERNAL_QSIZE : " + this.dataQ.size());
						try { Thread.sleep(1000); } catch (Exception ee) {return;}						
					}
					
					this.dataQ.addDataQ(vtDataSend);
					
					vtData = new Vector<ArrayList<String>>(); // clear Vector
				}
				
			} // while-end
			
			// ���� �ִ� ������ Send
			if ( vtData.size() > 0 ) {
				// dataQ�� ����
				Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
				
				while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // ���� Q size check
					XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
					try { Thread.sleep(1000); } catch (Exception ee) {return;}						
				}
				
				this.dataQ.addDataQ(vtDataSend);
				
				vtData = new Vector<ArrayList<String>>(); // clear Vector
			}
			
			// ���� ����� EOF ������ ����
			//ArrayList<String> arrayListEOF = new ArrayList<>(); // record Data	
			//arrayListEOF.add(null);
			//vtData.add(arrayListEOF);
			vtData.add(null);
			Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
			while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // ���� Q size check
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
			try { if ( this.oraConnObj != null ) this.oraConnObj.closeConnection(); } catch (Exception e) {} finally { this.oraConnObj = null; }
		}
		
		
		
	}
	
	// cksohn - xlim LOB Ÿ�� ����
	private String getReaderClob( Reader _reader ) {
        
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(_reader, XLConf.XL_APPLY_LOB_STREAM_BUFFERSIZE);

//            String line;
//            while(null != (line = br.readLine())) {
//                sb.append(line);
//            }
            // cksohn - CLOB read ��� ���� ���� start - [
            String line;
            char[] buffer = new char[XLConf.XL_APPLY_LOB_STREAM_BUFFERSIZE];
            int readCnt = 0;
            while( (readCnt = br.read(buffer)) > 0 ) {

          	  char[] readBuf = new char[readCnt];        	  
          	  System.arraycopy(buffer, 0, readBuf, 0, readCnt);
          	  sb.append( new String(readBuf) );
          	  
            }
            // ] - end cksohn - CLOB read ��� ���� ����
            
            br.close();
        } catch (Exception e) {
            // handle this exception
        } finally {
//            try{ _reader.close(); } catch (Exception e) {}
        }
        return sb.toString();
     
    }
	
	// cksohn - xlim LOB Ÿ�� ����
    private  byte[] getReaderBlob( InputStream _reader ) {
        
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
//	            byte[] buffer = new byte[65536];
            byte[] buffer = new byte[XLConf.XL_APPLY_LOB_STREAM_BUFFERSIZE];
            int readcount = 0;
            // ���Ϸκ��� �о���� ����Ʈ �迭�� ByteArrayOutputStream���� ����Ѵ�.
            while((readcount = _reader.read(buffer)) != -1){
                baos.write(buffer, 0, readcount);     
            }
            // ByteArrayOutputStream�� ���ΰ����� �����
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
