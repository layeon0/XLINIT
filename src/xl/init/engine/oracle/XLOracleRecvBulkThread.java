package xl.init.engine.oracle;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Vector;

import com.penta.scpdb.ScpDbAgent;

import oracle.jdbc.OraclePreparedStatement;
import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLOracleConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfo;
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

public class XLOracleRecvBulkThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;
	
	
	private XLOracleConnection oraConnObj = null;
	private OraclePreparedStatement pstmtSelect = null;
	
	
	// cksohn - BULK mode oracle sqlldr
	private RandomAccessFile pipe = null;
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLOracleRecvBulkThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		
		// ��� ���̺��� �÷� ����
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][RECV BULK]";
	}

	@Override
	public void run(){
		
		ResultSet rs = null;
		
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
			try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting RecvThread(Direct Patn Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
			// Oracle Connection ����
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();
			
			// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL start - [
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo(); 
			byte tdbType = tdbInfo.getDbType();
			// ] - end 
			
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
			
			byte[] bAry = null;
			
			// cksohn - BULK mode oracle sqlldr
			this.pipe = new RandomAccessFile( this.jobRunPol.getBulk_pipePath(), "rw" );
			
			
			// cksohn - BULK mode oracle sqlldr
			// TEST CODE!!!
			// FileWriter fw_test = new FileWriter("/tmp/ck.dat", false);
			
			
			long rowCnt = 0;
			
			long recvCnt = 0; // skip �Ѱ��� ������ select count �Ǽ�
			
			// cksohn - BULK mode oracle sqlldr
			StringBuffer sb_record = new StringBuffer(); // record Data

			while ( rs.next() ) {
			
				rowCnt++;
				// �̹� commit�� �Ǽ��� skip
				if ( rowCnt <= this.jobRunPol.getCondCommitCnt() ) {
					continue;
				}
				
				
				// ArrayList<String> arrayList = new ArrayList<>(); // record Data

				
				for (int i=0; i<this.vtColInfo.size(); i++) {
					
					XLJobColInfo colInfo = this.vtColInfo.get(i);
					switch ( colInfo.getDataType() ) {
					
					// cksohn - BULK mode oracle sqlldr
					// Bulk mode�� Recv���� CSV�� write�Ҷ� ���� ó���� �־�� ��
					case XLDicInfoCons.DATE:
						// .0 ����
						value = rs.getString(i+1);
						value = XLUtil.removeDot(value);
						
						// if (value != null && value.equals("")) {
						// if (value == null || value.equals("")) {
						// gssg - xl o2m ����
						// gssg - xl o2p bulk mode ����
						if ( value == null || value.equals("") ) {
							// gssg - o2t ����
							// gssg - o2t bulk mode ����
							if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
								value = ""; // oracle�� "" �� null
							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )
								value = "\\N"; // mysql�� \N �� null �� ��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							// gssg - ��� ����
							// gssg - O2P - \N�� ó��
//							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) 
//								// gssg - ppas bulk mode null ó��
//								value = "\\N";
						}
						
						break;
						
						// gssg - xl p2t ����
						// gssg - p2t �ϴٰ� o2m time zone ó��
						// gssg - o2o bulk tz ����
//						case XLDicInfoCons.TIMESTAMP_TZ:
						case XLDicInfoCons.TIMESTAMP_LTZ:
							
							value = rs.getString(i+1);							
							
							if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL ) {
								// time zone ���� ����
								value = XLUtil.removeMySQLTZ(value);								
							} else if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )  {
								
								// gssg - ���������ڿ������� �������̰� ���
								// gssg - Oracle to Oracle Ÿ���� ó��
								value = XLUtil.removeOracleTZ(value);
								
//								if ( value == null || value.equals("") ) {
//									// gssg - o2t ����
//									// gssg - o2t bulk mode ����
//									if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
//										value = ""; // oracle�� "" �� null
//
//								}
							}
							
							break;
							
					case XLDicInfoCons.CHAR:
					case XLDicInfoCons.NCHAR:
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);						
						// cksohn - xl oracle bulk mode ������ Ÿ�Ժ� ó�� ���� ���� - comment
						// value = XLUtil.removeDot(value);

						// gssg - ����� O2O
						// gssg - raw_to_varchar2 ��� ����						
						if ( value != null && XLConf.XL_CHAR_FUNCAPPLY 
								&& colInfo.getFunctionStr().toUpperCase().contains("RAW_TO_VARCHAR2") )  {
							// gssg - ����� O2O
							// gssg - raw_to_varchar2 ��� ����
							byte[] hexBytesChar =rs.getBytes(i+1);
							value = XLUtil.bytesToHexString(hexBytesChar);																					
						} else {
							// gssg - xl o2m ����
							// gssg - xl o2p bulk mode ����
							if( tdbType == XLCons.ORACLE ) {
								// gssg - o2t ����
								// gssg - o2t bulk mode ����
								value = XLUtil.replaceCharToCSV_ORACLE(value);							
							} else if( tdbType == XLCons.TIBERO ) { 
								// gssg - ������ ��浿 - tbloader Ư������ ó��
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							} else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							}
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode Ÿ�� ó��
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							}							
						}
						
						break;
					
					
					// gssg - o2o damo ����
					// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
					// gssg - char, nchar �и�
					case XLDicInfoCons.VARCHAR2:
					case XLDicInfoCons.VARCHAR:
						
						
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);						
						// cksohn - xl oracle bulk mode ������ Ÿ�Ժ� ó�� ���� ���� - comment
						// value = XLUtil.removeDot(value);

						
						// gssg - o2o damo ����
						if ( value != null && colInfo.getSecYN().equals("Y") ) {
							
							String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;

							ScpDbAgent agt = new ScpDbAgent();
							
							byte[] enc = null;
							
						    // gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
						    // gssg - damo api ����							    
						    if ( tdbInfo.getCharSet().contains("WIN949") || tdbInfo.getCharSet().contains("CP949")) {
						    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));							    	
						    } else if ( tdbInfo.getCharSet().contains("EUC") ) {
						    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("EUC-KR"));							    								    	
						    } else {
						    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("UTF-8"));							    								    	
						    }
							
//							enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));
							
							value = new String(enc);
							
						} else if ( value != null && XLConf.XL_CHAR_FUNCAPPLY 
								&& colInfo.getFunctionStr().toUpperCase().contains("RAW_TO_VARCHAR2") )  {
							// gssg - ����� O2O
							// gssg - raw_to_varchar2 ��� ����
							byte[] hexBytesChar =rs.getBytes(i+1);
							value = XLUtil.bytesToHexString(hexBytesChar);														
						}
						else {
							// gssg - xl o2m ����
							// gssg - xl o2p bulk mode ����
							if( tdbType == XLCons.ORACLE ) {
								// gssg - o2t ����
								// gssg - o2t bulk mode ����
								value = XLUtil.replaceCharToCSV_ORACLE(value);							
							} else if( tdbType == XLCons.TIBERO ) { 
								// gssg - ������ ��浿 - tbloader Ư������ ó��
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							} else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							}
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode Ÿ�� ó��
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							}
						}

																		
						break;

					// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
					// gssg - char, nchar �и�
					case XLDicInfoCons.NVARCHAR2:
					case XLDicInfoCons.NVARCHAR:
						
						
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);						
						// cksohn - xl oracle bulk mode ������ Ÿ�Ժ� ó�� ���� ���� - comment
						// value = XLUtil.removeDot(value);

						
						// gssg - o2o damo ����
						if ( value != null && colInfo.getSecYN().equals("Y") ) {
							
							String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;

							ScpDbAgent agt = new ScpDbAgent();
							
							byte[] enc = null;
							
						    // gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
						    // gssg - damo api ����							    
						    if ( tdbInfo.getnCharSet().contains("WIN949") || tdbInfo.getnCharSet().contains("CP949")) {
						    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));					    	
						    } else if ( tdbInfo.getCharSet().contains("EUC") ) {
						    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("EUC-KR"));							    								    	
						    } else {
						    	enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("UTF-8"));							    								    	
						    }
							
//							enc = agt.ScpEncStr(iniFilePath, "KEY2", value.getBytes("MS949"));
							
							value = new String(enc);
							
						} else if ( value != null && XLConf.XL_CHAR_FUNCAPPLY 
								&& colInfo.getFunctionStr().toUpperCase().contains("RAW_TO_VARCHAR2") )  {
							// gssg - ����� O2O
							// gssg - raw_to_varchar2 ��� ����
							byte[] hexBytesChar =rs.getBytes(i+1);
							value = XLUtil.bytesToHexString(hexBytesChar);														
						} else {
							// gssg - xl o2m ����
							// gssg - xl o2p bulk mode ����
							if( tdbType == XLCons.ORACLE ) {
								// gssg - o2t ����
								// gssg - o2t bulk mode ����
								value = XLUtil.replaceCharToCSV_ORACLE(value);							
							} else if( tdbType == XLCons.TIBERO ) { 
								// gssg - ������ ��浿 - tbloader Ư������ ó��
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							} else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							}
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode Ÿ�� ó��
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							}
						}

																		
						break;
						
					case XLDicInfoCons.CLOB:
					case XLDicInfoCons.NCLOB:
					case XLDicInfoCons.LONG:
					case XLDicInfoCons.XMLTYPE:
						// TODO - cksohn - BULK mode oracle sqlldr - Hexa�� ��ȯ�� X'4141414 �� ���� ���·� ��ȯ!!!
						value = rs.getString(i+1);

						// gssg - xl o2m ����
						// gssg - xl o2p bulk mode ����
						if ( value == null || value.equals("") ) {
							if( tdbType == XLCons.ORACLE )
								value = ""; // oracle�� "" �� null
							else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )
								value = "\\N"; // mysql�� \N �� null �� ��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��	
							// gssg - ��� ����
							// gssg - O2P - \N�� ó��
//							else if( tdbType == XLCons.PPAS || 
//									tdbType == XLCons.POSTGRESQL ) // gssg - ppas bulk mode Ÿ�� ó��
//								value = "\\N"; // ppas�� \N �� null �� ��
						} else {
							if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL ) {
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							} 
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��					
							else if( tdbType == XLCons.PPAS || 
									tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode Ÿ�� ó��
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							} 
						}
						
						break;
					
					case XLDicInfoCons.RAW:
					case XLDicInfoCons.LONGRAW:
					case XLDicInfoCons.BLOB:
						// TODO - cksohn - BULK mode oracle sqlldr
						// binary Ÿ�� (hex-->binary)
												
							bAry = rs.getBytes(i+1);
							
							// gssg - xl o2m ����
							// gssg - xl o2p bulk mode ����
							if ( bAry != null && tdbType == XLCons.ORACLE ) { // ����Ŭ�� ���̳ʸ� ���� �ȵ�
								value = null;
							} else if(tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) {
								if (bAry == null ) {									
									value = "\\N"; // mysql�� \N �� null �� ��								
								} else {
									// gssg - xl o2m ����
									// gssg - o2m bulk mode ���̳ʸ� Ÿ�� ����									
									value = XLUtil.bytesToHexString(bAry);																		
									} 									
							} 
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode Ÿ�� ó��
								// gssg - ��� ����
								// gssg - O2P - \N�� ó��
//								if (bAry == null) { // gssg - ppas bulk mode null ó��
//									value = "\\N";
//								} else {
//									value = "\\x" + XLUtil.bytesToHexString(bAry);	//  ppas \x �ٿ���
//								}
								if(bAry != null) {
									value = "\\x" + XLUtil.bytesToHexString(bAry);
								}								
							}
													
						break;												

						
					default : 

						
						value = rs.getString(i+1);
						
						// if (value != null && value.equals("")) {
						// cksohn - BULK mode oracle sqlldr
						// if (value == null || value.equals("")) {
						// gssg - xl o2m ����
						if (value == null || value.equals("")) {
							// gssg - xl o2m ����
							// gssg - o2t ����
							// gssg - o2t bulk mode ����
							if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
								value = ""; // oracle�� "" �� null
							// gssg - xl o2p bulk mode ����
							// gssg - ppas bulk mode null ó��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��					
							// gssg - ��� ����
							// gssg - O2P - \N�� ó��
//							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL 
//									|| tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL )
							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )								
								value = "\\N"; // mysql�� \N �� null �� ��								
						}
												
						break;
						
					}	
					// arrayList.add(value);
					// cksohn - BULK mode oracle sqlldr
					if ( i != 0 ) {
						sb_record.append(",");
					}
					
					// gssg - xl o2p bulk mode ����
					// gssg - ppas bulk mode null ó��
					// gssg - xl ��ü������ ����2
					// gssg - PostgreSQL Ŀ���� ó��				
					// gssg - ��� ����
					// gssg - O2P - \N�� ó��
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

					
										
				} // for-end 1���� record ���� ����
				

				// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
				// if ( tdbType == XLCons.ORACLE ) {
				// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL - �̻�� �ɼ�
				if ( tdbType == XLCons.ORACLE && !XLConf.XL_BULK_ORACLE_EOL.equals("")) {
					
					sb_record.append(XLConf.XL_BULK_ORACLE_EOL);
				} else {
					// cksohn - xl bulk mode for oracle - ��� ó�� ���� ����
					// Ÿ���� ����Ŭ�ϰ��� \n write ����
					sb_record.append("\n");
				}
				
				
				// XLLogger.outputInfoLog("CKSOHN DEBUG sb_record = " + sb_record.toString());
				
				// cksohn - BULK mode oracle sqlldr ���⼭ pipe�� write ??!?! - ��Ҵٰ� �ѹ��� �ϴ°� ������ ???
				// vtData.add(arrayList);
				// pipe.writeBytes(sb_record.toString());
				
				
				// TEST CODE
				// fw_test.write(sb_record.toString());
				
				recvCnt++;
				
				// cksohn - BULK mode oracle sqlldr
				// if ( (recvCnt % XLConf.XL_MGR_SEND_COUNT ) == 0) {
				// cksohn - xl bulk mode ���� ���� - t2o
				if ( (recvCnt % XLConf.XL_BATCH_SIZE ) == 0) {
					
					
					// pipe.writeBytes(sb_record.toString());
					// cksohn - xl bulk mode for oracle - special character loading error
					pipe.write(sb_record.toString().getBytes("UTF-8"));
					
					// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());

					sb_record = new StringBuffer(); // �ʱ�ȭ
														
				}
				
				
				
			} // while-end
			
			// remain data ó�� 
			if ( sb_record.toString().length() > 0 ) {
				// ������ ������ ���� 
				// pipe.writeBytes(sb_record.toString());
				// cksohn - xl bulk mode for oracle - special character loading error
				pipe.write(sb_record.toString().getBytes("UTF-8"));
				
				// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());
			}
			
			XLLogger.outputInfoLog("[]");
			
			// TEST CODE
			//fw_test.close();
			
			// cksohn - BULK mode oracle sqlldr - comment
			// ���� �ִ� ������ Send
//			if ( vtData.size() > 0 ) {
//				// dataQ�� ����
//				Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
//				
//				while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // ���� Q size check
//					XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
//					try { Thread.sleep(1000); } catch (Exception ee) {return;}						
//				}
//				
//				this.dataQ.addDataQ(vtDataSend);
//				
//				vtData = new Vector<ArrayList<String>>(); // clear Vector
//			}
//			
//			// ���� ����� EOF ������ ����
//			//ArrayList<String> arrayListEOF = new ArrayList<>(); // record Data	
//			//arrayListEOF.add(null);
//			//vtData.add(arrayListEOF);
//			vtData.add(null);
//			Vector<ArrayList<String>> vtDataSend = (Vector<ArrayList<String>>)vtData.clone();
//			while ( this.dataQ.size() >= XLConf.XL_MGR_INTERNAL_QSIZE ) { // ���� Q size check
//				XLLogger.outputInfoLog(this.logHead +  " INTERNAL_QSIZE : " + this.dataQ.size());
//				try { Thread.sleep(1000); } catch (Exception ee) {return;}						
//			}
//			
//			this.dataQ.addDataQ(vtDataSend);
			
			
			// gssg - o2t ����
			// gssg - o2t bulk mode ����
//			if(this.jobRunPol.getTdbInfo().getDbType() == XLCons.TIBERO) {				
//				this.jobRunPol.setWritePipe(true);
//				XLLogger.outputInfoLog("&&&&&&&&&&&&&&&&&&&&&&[GSSG DEBUG] this.jobRunPol.setWritePipe(true);&&&&&&&&&&&&&&");															
//			}
			


			etime = System.currentTimeMillis();
			
			long elapsedTime = (etime-stime) / 1000;
			
			//XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Completed Job Recv : " + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("\tTotal Select count : " + recvCnt + " / Elapsed time(sec) : " +  elapsedTime);
			//XLLogger.outputInfoLog("");
			
			// gssg - o2m bulk mode logging ����
			this.jobRunPol.setApplyCnt(recvCnt); // xl recvCnt�� applyCnt�� ����

			// cksohn - BULK mode oracle sqlldr
			
			return;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {
			
			// gssg - o2t ����
			// gssg - o2t bulk mode ����
//			if(this.jobRunPol.getTdbInfo().getDbType() == XLCons.TIBERO) {				
//
//				int chkCnt = 0;
//
//				if ( XLConf.XL_MGR_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] ----- START isLoadQuery WHILE!!!!! - " + this.jobRunPol.isLoadQuery());
//				}
//					try {
//						while ( !this.jobRunPol.isLoadQuery() && chkCnt <= 5 ) {
//
//						chkCnt++;
//						XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][RECV BULK] Waiting Check Loader State.(" + chkCnt + ")");					
//						Thread.sleep(2000);
//						}					
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				if ( XLConf.XL_MGR_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] ----- END isLoadQuery WHILE!!!! - " + this.jobRunPol.isLoadQuery());
//				}	
//				
//			}
			
			// cksohn - BULK mode oracle sqlldr
			try { if (pipe != null) pipe.close(); } catch (Exception e1) {} finally { pipe = null; }			
			
			try { if ( this.pstmtSelect != null ) this.pstmtSelect.close(); } catch (Exception e) {} finally { this.pstmtSelect = null; }
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }
			
			try { if ( this.oraConnObj != null ) this.oraConnObj.closeConnection(); } catch (Exception e) {} finally { this.oraConnObj = null; }
			
			// cksohn - BULK mode oracle sqlldr
			//this.dataQ.notifyEvent();

			
			XLLogger.outputInfoLog(" !   BulkThread END  !");
			
		}
		
		
		
	}
}
