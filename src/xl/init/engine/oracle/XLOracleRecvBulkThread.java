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
	
	// 테이블 컬럼의 정보
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
					switch ( colInfo.getDataType() ) {
					
					// cksohn - BULK mode oracle sqlldr
					// Bulk mode는 Recv에서 CSV로 write할때 부터 처리해 주어야 함
					case XLDicInfoCons.DATE:
						// .0 제거
						value = rs.getString(i+1);
						value = XLUtil.removeDot(value);
						
						// if (value != null && value.equals("")) {
						// if (value == null || value.equals("")) {
						// gssg - xl o2m 지원
						// gssg - xl o2p bulk mode 지원
						if ( value == null || value.equals("") ) {
							// gssg - o2t 지원
							// gssg - o2t bulk mode 지원
							if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
								value = ""; // oracle은 "" 도 null
							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )
								value = "\\N"; // mysql은 \N 이 null 로 들어감
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리
							// gssg - 모듈 보완
							// gssg - O2P - \N값 처리
//							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) 
//								// gssg - ppas bulk mode null 처리
//								value = "\\N";
						}
						
						break;
						
						// gssg - xl p2t 지원
						// gssg - p2t 하다가 o2m time zone 처리
						// gssg - o2o bulk tz 보완
//						case XLDicInfoCons.TIMESTAMP_TZ:
						case XLDicInfoCons.TIMESTAMP_LTZ:
							
							value = rs.getString(i+1);							
							
							if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL ) {
								// time zone 내용 제거
								value = XLUtil.removeMySQLTZ(value);								
							} else if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )  {
								
								// gssg - 국가정보자원관리원 데이터이관 사업
								// gssg - Oracle to Oracle 타임존 처리
								value = XLUtil.removeOracleTZ(value);
								
//								if ( value == null || value.equals("") ) {
//									// gssg - o2t 지원
//									// gssg - o2t bulk mode 지원
//									if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
//										value = ""; // oracle은 "" 도 null
//
//								}
							}
							
							break;
							
					case XLDicInfoCons.CHAR:
					case XLDicInfoCons.NCHAR:
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);						
						// cksohn - xl oracle bulk mode 데이터 타입별 처리 오류 수정 - comment
						// value = XLUtil.removeDot(value);

						// gssg - 대법원 O2O
						// gssg - raw_to_varchar2 기능 지원						
						if ( value != null && XLConf.XL_CHAR_FUNCAPPLY 
								&& colInfo.getFunctionStr().toUpperCase().contains("RAW_TO_VARCHAR2") )  {
							// gssg - 대법원 O2O
							// gssg - raw_to_varchar2 기능 지원
							byte[] hexBytesChar =rs.getBytes(i+1);
							value = XLUtil.bytesToHexString(hexBytesChar);																					
						} else {
							// gssg - xl o2m 지원
							// gssg - xl o2p bulk mode 지원
							if( tdbType == XLCons.ORACLE ) {
								// gssg - o2t 지원
								// gssg - o2t bulk mode 지원
								value = XLUtil.replaceCharToCSV_ORACLE(value);							
							} else if( tdbType == XLCons.TIBERO ) { 
								// gssg - 현대차 대방동 - tbloader 특수문자 처리
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							} else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							}
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode 타입 처리
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							}							
						}
						
						break;
					
					
					// gssg - o2o damo 적용
					// gssg - damo 캐릭터셋 하드 코딩 보완
					// gssg - char, nchar 분리
					case XLDicInfoCons.VARCHAR2:
					case XLDicInfoCons.VARCHAR:
						
						
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);						
						// cksohn - xl oracle bulk mode 데이터 타입별 처리 오류 수정 - comment
						// value = XLUtil.removeDot(value);

						
						// gssg - o2o damo 적용
						if ( value != null && colInfo.getSecYN().equals("Y") ) {
							
							String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;

							ScpDbAgent agt = new ScpDbAgent();
							
							byte[] enc = null;
							
						    // gssg - damo 캐릭터셋 하드 코딩 보완
						    // gssg - damo api 적용							    
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
							// gssg - 대법원 O2O
							// gssg - raw_to_varchar2 기능 지원
							byte[] hexBytesChar =rs.getBytes(i+1);
							value = XLUtil.bytesToHexString(hexBytesChar);														
						}
						else {
							// gssg - xl o2m 지원
							// gssg - xl o2p bulk mode 지원
							if( tdbType == XLCons.ORACLE ) {
								// gssg - o2t 지원
								// gssg - o2t bulk mode 지원
								value = XLUtil.replaceCharToCSV_ORACLE(value);							
							} else if( tdbType == XLCons.TIBERO ) { 
								// gssg - 현대차 대방동 - tbloader 특수문자 처리
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							} else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							}
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode 타입 처리
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							}
						}

																		
						break;

					// gssg - damo 캐릭터셋 하드 코딩 보완
					// gssg - char, nchar 분리
					case XLDicInfoCons.NVARCHAR2:
					case XLDicInfoCons.NVARCHAR:
						
						
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);						
						// cksohn - xl oracle bulk mode 데이터 타입별 처리 오류 수정 - comment
						// value = XLUtil.removeDot(value);

						
						// gssg - o2o damo 적용
						if ( value != null && colInfo.getSecYN().equals("Y") ) {
							
							String iniFilePath = XLConf.XL_TAR_KEYFILE_PATH;

							ScpDbAgent agt = new ScpDbAgent();
							
							byte[] enc = null;
							
						    // gssg - damo 캐릭터셋 하드 코딩 보완
						    // gssg - damo api 적용							    
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
							// gssg - 대법원 O2O
							// gssg - raw_to_varchar2 기능 지원
							byte[] hexBytesChar =rs.getBytes(i+1);
							value = XLUtil.bytesToHexString(hexBytesChar);														
						} else {
							// gssg - xl o2m 지원
							// gssg - xl o2p bulk mode 지원
							if( tdbType == XLCons.ORACLE ) {
								// gssg - o2t 지원
								// gssg - o2t bulk mode 지원
								value = XLUtil.replaceCharToCSV_ORACLE(value);							
							} else if( tdbType == XLCons.TIBERO ) { 
								// gssg - 현대차 대방동 - tbloader 특수문자 처리
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							} else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							}
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode 타입 처리
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							}
						}

																		
						break;
						
					case XLDicInfoCons.CLOB:
					case XLDicInfoCons.NCLOB:
					case XLDicInfoCons.LONG:
					case XLDicInfoCons.XMLTYPE:
						// TODO - cksohn - BULK mode oracle sqlldr - Hexa로 변환후 X'4141414 과 같은 형태로 변환!!!
						value = rs.getString(i+1);

						// gssg - xl o2m 지원
						// gssg - xl o2p bulk mode 지원
						if ( value == null || value.equals("") ) {
							if( tdbType == XLCons.ORACLE )
								value = ""; // oracle은 "" 도 null
							else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )
								value = "\\N"; // mysql은 \N 이 null 로 들어감
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리	
							// gssg - 모듈 보완
							// gssg - O2P - \N값 처리
//							else if( tdbType == XLCons.PPAS || 
//									tdbType == XLCons.POSTGRESQL ) // gssg - ppas bulk mode 타입 처리
//								value = "\\N"; // ppas는 \N 이 null 로 들어감
						} else {
							if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL ) {
								value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
							} 
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리					
							else if( tdbType == XLCons.PPAS || 
									tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode 타입 처리
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							} 
						}
						
						break;
					
					case XLDicInfoCons.RAW:
					case XLDicInfoCons.LONGRAW:
					case XLDicInfoCons.BLOB:
						// TODO - cksohn - BULK mode oracle sqlldr
						// binary 타입 (hex-->binary)
												
							bAry = rs.getBytes(i+1);
							
							// gssg - xl o2m 지원
							// gssg - xl o2p bulk mode 지원
							if ( bAry != null && tdbType == XLCons.ORACLE ) { // 오라클은 바이너리 지원 안됨
								value = null;
							} else if(tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) {
								if (bAry == null ) {									
									value = "\\N"; // mysql은 \N 이 null 로 들어감								
								} else {
									// gssg - xl o2m 보완
									// gssg - o2m bulk mode 바이너리 타입 보완									
									value = XLUtil.bytesToHexString(bAry);																		
									} 									
							} 
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리					
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) { // gssg - ppas bulk mode 타입 처리
								// gssg - 모듈 보완
								// gssg - O2P - \N값 처리
//								if (bAry == null) { // gssg - ppas bulk mode null 처리
//									value = "\\N";
//								} else {
//									value = "\\x" + XLUtil.bytesToHexString(bAry);	//  ppas \x 붙여줌
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
						// gssg - xl o2m 지원
						if (value == null || value.equals("")) {
							// gssg - xl o2m 지원
							// gssg - o2t 지원
							// gssg - o2t bulk mode 지원
							if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
								value = ""; // oracle은 "" 도 null
							// gssg - xl o2p bulk mode 지원
							// gssg - ppas bulk mode null 처리
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리					
							// gssg - 모듈 보완
							// gssg - O2P - \N값 처리
//							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL 
//									|| tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL )
							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )								
								value = "\\N"; // mysql은 \N 이 null 로 들어감								
						}
												
						break;
						
					}	
					// arrayList.add(value);
					// cksohn - BULK mode oracle sqlldr
					if ( i != 0 ) {
						sb_record.append(",");
					}
					
					// gssg - xl o2p bulk mode 지원
					// gssg - ppas bulk mode null 처리
					// gssg - xl 전체적으로 보완2
					// gssg - PostgreSQL 커넥터 처리				
					// gssg - 모듈 보완
					// gssg - O2P - \N값 처리
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
				

				// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
				// if ( tdbType == XLCons.ORACLE ) {
				// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL - 미사용 옵션
				if ( tdbType == XLCons.ORACLE && !XLConf.XL_BULK_ORACLE_EOL.equals("")) {
					
					sb_record.append(XLConf.XL_BULK_ORACLE_EOL);
				} else {
					// cksohn - xl bulk mode for oracle - 결과 처리 오류 수정
					// 타겟이 오라클일경우는 \n write 안함
					sb_record.append("\n");
				}
				
				
				// XLLogger.outputInfoLog("CKSOHN DEBUG sb_record = " + sb_record.toString());
				
				// cksohn - BULK mode oracle sqlldr 여기서 pipe에 write ??!?! - 모았다가 한번애 하는게 낳을까 ???
				// vtData.add(arrayList);
				// pipe.writeBytes(sb_record.toString());
				
				
				// TEST CODE
				// fw_test.write(sb_record.toString());
				
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
			
			XLLogger.outputInfoLog("[]");
			
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
			
			
			// gssg - o2t 지원
			// gssg - o2t bulk mode 지원
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
			
			// gssg - o2m bulk mode logging 지원
			this.jobRunPol.setApplyCnt(recvCnt); // xl recvCnt를 applyCnt로 적용

			// cksohn - BULK mode oracle sqlldr
			
			return;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {
			
			// gssg - o2t 지원
			// gssg - o2t bulk mode 지원
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
