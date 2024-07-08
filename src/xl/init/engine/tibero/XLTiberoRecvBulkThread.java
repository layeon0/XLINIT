package xl.init.engine.tibero;

// cksohn - xl tibero src ��� �߰�

import java.io.RandomAccessFile;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Vector;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLTiberoConnection;
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

public class XLTiberoRecvBulkThread extends Thread {

	
	private XLDataQ dataQ = null;
	private XLJobRunPol jobRunPol = null;
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;

	
	private XLTiberoConnection tiberoConnObj = null;
	private PreparedStatement pstmtSelect = null;

	
	// cksohn - BULK mode oracle sqlldr
	private RandomAccessFile pipe = null;
	
	
	public boolean isWait = false;
	
	
	private String errMsg = null;
	
	private String logHead = "";
	
	public XLTiberoRecvBulkThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		this.dataQ = this.jobRunPol.getDataQ();
		
		// ��� ���̺��� �÷� ����
		this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][RECV BULK]";
		

	}

	@Override
	public void run(){
		
		
		// ################################ CKSOHN DEBUG TEST CODE
		//XLLogger.outputInfoLog("CKSOHN DEBUG TEST CODE!!!!");
		//XLCsvFileDebug csvFileDebug = new XLCsvFileDebug();
		//csvFileDebug.writerInit(XLManager.XL_DIR + File.separator + "pipe" + File.separator + "debugData.csv", false);
		
		// ##########################################################
		
		ResultSet rs = null;
		
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting RecvThread(Direct Patn Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
//			XLLogger.outputInfoLog("CKSOHN DEBUG #### sleep 5 secs");
//			Thread.sleep(5000);
			
			// Oracle Connection ����
			XLDBMSInfo sdbInfo = this.jobRunPol.getSdbInfo();
			
			// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL start - [
			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo(); 
			byte tdbType = tdbInfo.getDbType();
			// ] - end 
			
			tiberoConnObj = new XLTiberoConnection(
					sdbInfo.getIp(), 
					sdbInfo.getDbSid(),
					sdbInfo.getUserId(),
					sdbInfo.getPasswd(),
					sdbInfo.getPort(),
					sdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			if ( !tiberoConnObj.makeConnection() ) {
				
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
			
			this.pstmtSelect = (PreparedStatement)tiberoConnObj.getConnection().prepareStatement(this.jobRunPol.getSrcSelectSql());
			
			
//			XLLogger.outputInfoLog("CKSOHN DEBUG ######  SRC QRY = " + this.jobRunPol.getSrcSelectSql());
			
			// cksohn - xl tibero src ��� �߰� - fetch size ����
			this.pstmtSelect.setFetchSize(XLConf.XL_FETCH_SIZE);
			
			stime = System.currentTimeMillis();
			
			rs = this.pstmtSelect.executeQuery();
			
			Vector<ArrayList<String>> vtData = new Vector<ArrayList<String>>();
			
			String value = null;
			
			
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
					
					// binary Ÿ�� (hex-->binary)
//					case XLDicInfoCons.RAW:
//					
//						byte[] bAry = rs.getBytes(i+1);
//						
//						if ( bAry != null ) {
//							value = null;
//						} else {
//							value = XLUtil.bytesToHexString(bAry);
//						}
//						
//						break;
						
					// LONG/LONGRAW �� LOB Ÿ���� ���߿� ó��....
					
					// cksohn - BULK mode oracle sqlldr
					// Bulk mode�� Recv���� CSV�� write�Ҷ� ���� ó���� �־�� ��
					case XLDicInfoCons.DATE:
						// .0 ����
						value = rs.getString(i+1);
						value = XLUtil.removeDot(value);
						
						// if (value != null && value.equals("")) {
						// if (value == null || value.equals("")) {
						// gssg - xl t2p ����
						// gssg - t2p bulk mode Ÿ�� ó��
						if (value == null || value.equals("")) {
							if (tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO)
								value = ""; // tibero�� "" �� null							
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - t2m bulk mode ����
							else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL )
								value = "\\N"; // mysql�� \N �� null �� ��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							// gssg - �Ｚ���� - \N�� ó��
//							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL )
//								value = "\\N";								
						}
						
						break;

					case XLDicInfoCons.CHAR:
					case XLDicInfoCons.VARCHAR2:
					case XLDicInfoCons.VARCHAR:
					case XLDicInfoCons.NCHAR:
					case XLDicInfoCons.NVARCHAR2:
						// cksohn - BULK mode oracle sqlldr - Replace "
						value = rs.getString(i+1);
						
						// cksohn - xl oracle bulk mode ������ Ÿ�Ժ� ó�� ���� ���� - comment
						// value = XLUtil.removeDot(value);
						
						
						// XLLogger.outputInfoLog("CKSOHN DEBUG value = "  + value);
						
						// gssg - xl t2p ����
						// gssg - t2p bulk mode Ÿ�� ó��
						// gssg - o2m  �ϴٰ� t2t bulk mode ����
						if ( tdbType == XLCons.ORACLE ) {
							value = XLUtil.replaceCharToCSV_ORACLE(value);							
						} else if ( tdbType == XLCons.TIBERO ) {
							value = XLUtil.replaceCharToCSV_TIBERO(value);
						} 
						// gssg - xl ��ü������ ����2
						// gssg - PostgreSQL Ŀ���� ó��						
						else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) {
							value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
						}
						else if( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL) { 
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - t2m bulk mode ����
							value = XLUtil.replaceCharToCSV_OracleToMySQL(value);
						}
						


						
//						if (value != null && value.equals("")) {
//							// value = null; // oracle�� "" �� null
//							value = ""; // 
//						}
						
						break;
						
					case XLDicInfoCons.CLOB:
					case XLDicInfoCons.NCLOB:
					case XLDicInfoCons.LONG:
					case XLDicInfoCons.XMLTYPE:
						// TODO - cksohn - BULK mode oracle sqlldr - Hexa�� ��ȯ�� X'4141414 �� ���� ���·� ��ȯ!!!
						value = rs.getString(i+1);
						
						// gssg - xl t2p ����
						// gssg - t2p bulk mode Ÿ�� ó��
						// gssg - o2m �ϴٰ� t2t bulk mode ����
						if ( value == null || value.equals("") ) {
							if ( tdbType == XLCons.ORACLE || tdbType == XLCons.TIBERO )
								value = ""; // tibero�� "" �� null
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							// gssg - �Ｚ���� - \N�� ó��
//							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL )
//								value = "\\N";
						} 
						else {
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) {
								value = XLUtil.replaceCharToCSV_OracleToPPAS(value);
							} else if ( tdbType == XLCons.TIBERO ) {
								value = XLUtil.replaceCharToCSV_TIBERO(value);
							}
						}
						
						break;

					case XLDicInfoCons.RAW:
					case XLDicInfoCons.LONGRAW:
					case XLDicInfoCons.BLOB:
						// TODO - cksohn - BULK mode oracle sqlldr
						// binary Ÿ�� (hex-->binary)
						
							byte[] bAry = rs.getBytes(i+1);
							
							// gssg - xl t2p ����
							// gssg - t2p bulk mode Ÿ�� ó��						
							if ( bAry != null && (tdbType == XLCons.ORACLE 
									|| tdbType == XLCons.TIBERO) ) { // ����Ŭ�� Ƽ���δ� ���̳ʸ� ���� �ȵ�
								value = null;
							} 
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) {
								// gssg - �Ｚ���� - \N�� ó��
//								if(bAry == null) {
//									value = "\\N";
//								} else {
//									value = "\\x" + XLUtil.bytesToHexString(bAry);
//								}
								if(bAry != null) {
									value = "\\x" + XLUtil.bytesToHexString(bAry);
								}								
								
							}
							
						break;

					case XLDicInfoCons.TIMESTAMP_TZ:
							
						value = rs.getString(i+1);							
							
						if ( tdbType == XLCons.ORACLE ) {							
							value = XLUtil.removeOracleTZ(value);							
						} else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL ) {
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - t2m bulk mode ����
							value = XLUtil.removeMySQLTZ(value);								
						}
						
						break;

					// gssg - ���������ڿ������� �������̰� ���
					// gssg - Tibero to Oracle Ÿ���� ó��
					case XLDicInfoCons.TIMESTAMP_LTZ:
						
						value = rs.getString(i+1);							
						
						if ( tdbType == XLCons.ORACLE ) {							
							value = XLUtil.removeOracleTZ(value);							
						} else if ( tdbType == XLCons.MARIADB || tdbType == XLCons.MYSQL ) {
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - t2m bulk mode ����
							if (value == null || value.equals("")) {
								value = "\\N";
							}
							
						}
						
						break;
						
						
					default : 
						value = rs.getString(i+1);
						
						// gssg - xl t2p ����
						// gssg - t2p bulk mode Ÿ�� ó��						
						if ( value == null || value.equals("") ) {
							
							if ( tdbType == XLCons.ORACLE )
								value = ""; // tibero�� "" �� null
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							// gssg - �Ｚ���� - \N�� ó��
//							else if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL )
//								value = "\\N";
							
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - t2m bulk mode ����
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
					
					// gssg - xl t2p ����
					// gssg - t2p bulk mode Ÿ�� ó��
					// gssg - xl ��ü������ ����2
					// gssg - PostgreSQL Ŀ���� ó��
//					if ( (tdbType != XLCons.PPAS || !value.equals("\\N")) || 
//							(tdbType != XLCons.POSTGRESQL || !value.equals("\\N")) ) {
//						sb_record.append("\"").append(value).append("\"");						
//					}
					
					// gssg - �Ｚ���� - \N�� ó��
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

				if ( tdbType == XLCons.ORACLE &&  !XLConf.XL_BULK_ORACLE_EOL.equals("")) {
					
					sb_record.append(XLConf.XL_BULK_ORACLE_EOL);
					
				} else {
					// cksohn - xl bulk mode for oracle - ��� ó�� ���� ����
					// Ÿ���� ����Ŭ�ϰ��� \n write ����
					sb_record.append("\n");
				}
				
				// cksohn - BULK mode oracle sqlldr ���⼭ pipe�� write ??!?! - ��Ҵٰ� �ѹ��� �ϴ°� ������ ???
				// vtData.add(arrayList);
				// pipe.writeBytes(sb_record.toString());
				
				
				// TEST CODE
				// fw_test.write(sb_record.toString());
//				 XLLogger.outputInfoLog("CKSOHN DEBUG sb_record = " + sb_record.toString());

				recvCnt++;
				
				
				// cksohn - BULK mode oracle sqlldr
				// cksohn - xl bulk mode ���� ���� - t2o - �Ǵ����� write �ϵ���
				// if ( (recvCnt % XLConf.XL_MGR_SEND_COUNT ) == 0) {					
				// if ( (recvCnt % 50 ) == 0) {
				if ( (recvCnt % XLConf.XL_BATCH_SIZE ) == 0) {
					
					// pipe.writeBytes(sb_record.toString());
					// cksohn - xl bulk mode for oracle - special character loading error
					pipe.write(sb_record.toString().getBytes("UTF-8"));
					
					// CKSOHN DEBUG TEST CODE !!!!!!!!!!!!!!!!!!!!
					// csvFileDebug.writeCSVFile(sb_record.toString());
					
					// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());

					sb_record = new StringBuffer(); // �ʱ�ȭ
					
					
					
				}								
				
			} // while-end
			
			// cksohn - xl bulk mode ���� ���� - t2o - comment - �Ǵ����� write  �ϸ� �Ʒ� �κ� ���ʿ�
			
			// remain data ó�� 
			if ( sb_record.toString().length() > 0 ) {
				// ������ ������ ���� 
				// pipe.writeBytes(sb_record.toString());
				// cksohn - xl bulk mode for oracle - special character loading error
				pipe.write(sb_record.toString().getBytes("UTF-8"));
				
				// CKSOHN DEBUG TEST CODE !!!!!!!!!!!!!!!!!!!!
				// csvFileDebug.writeCSVFile(sb_record.toString());
				
				// XLLogger.outputInfoLog(this.logHead + "Recv DI Send data size = " +  sb_record.length());
			}
			
			
			// gssg - xl t2t ����
			// gssg - t2t bulk mode ������ ���� ����
			this.jobRunPol.setWrite(true);
			
			etime = System.currentTimeMillis();
			
			long elapsedTime = (etime-stime) / 1000;
			
			//XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Completed Job Recv : " + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("\tTotal Select count : " + recvCnt + " / Elapsed time(sec) : " +  elapsedTime);
			//XLLogger.outputInfoLog("");
			
			// gssg - ���������ڿ������� �������̰� ���
			// gssg - t2m bulk mode ����
			this.jobRunPol.setApplyCnt(recvCnt);

			// cksohn - BULK mode oracle sqlldr
			this.dataQ.notifyEvent();
			
			return;

			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			errMsg = e.toString();
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			this.jobRunPol.setErrMsg_Recv(errMsg);
			
		} finally {
			
//			this.jobRunPol.setrunRecv(true);
				
			
			// gssg - xl t2t ����
			// gssg - t2t bulk mode ������ ���� ����
			// gssg - xl t2p ����						
//			if(this.jobRunPol.getTdbInfo().getDbType() == XLCons.TIBERO) {	
//				
//				int chkCnt = 0;
//
//				if ( XLConf.XL_MGR_DEBUG_YN ) {
//					XLLogger.outputInfoLog("[DEBUG] ----- START isLoadQuery WHILE!!!!! - " + this.jobRunPol.isLoadQuery());
//				}
//					try {
//						while ( !this.jobRunPol.isLoadQuery() && chkCnt <= 10 ) {
//
//						chkCnt++;
//						XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][RECV BULK] Waiting Check Loader State.(" + chkCnt + ")");					
//						Thread.sleep(1000);
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
			
		
			try { if (pipe != null) pipe.close(); } catch (Exception e1) {} finally { pipe = null; }

			try { if ( this.pstmtSelect != null ) this.pstmtSelect.close(); } catch (Exception e) {} finally { this.pstmtSelect = null; }
			
			try { if ( rs != null ) rs.close(); } catch (Exception e) {} finally { rs = null; }
			
			try { if ( this.tiberoConnObj != null ) this.tiberoConnObj.closeConnection(); } catch (Exception e) {} finally { this.tiberoConnObj = null; }
			
			
			// CKSOHN DEBUG TEST CODE !!!!!!!!!!!!!!!!!!!!
			// csvFileDebug.writerClose();
			
			// cksohn - BULK mode oracle sqlldr
			this.dataQ.notifyEvent();			

		}
		
		
		
	}
}
