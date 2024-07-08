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
	
	// ���̺� �÷��� ����
	private Vector<XLJobColInfo> vtColInfo = null;

	// gssg - t2p ����
	// gssg - postgreSQL Ŀ���� ����
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
			
			// gssg - xl m2m bulk mode ����
			postgreSQLConnObj = new XLPostgreSQLConnection(
					sdbInfo.getIp(), 
					sdbInfo.getDbSid(),
					sdbInfo.getUserId(),
					sdbInfo.getPasswd(),
					sdbInfo.getPort(),
					sdbInfo.getDbType() 
					);
			
			
			// Target DB Connection
			// gssg - xl m2m bulk mode ����
			if ( !postgreSQLConnObj.makeConnection() ) {
				
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
			
			// gssg - xl m2m bulk mode ����
			this.pstmtSelect = (PreparedStatement)postgreSQLConnObj.getConnection().prepareStatement(this.jobRunPol.getSrcSelectSql());
			
						
			stime = System.currentTimeMillis();
			
			this.pstmtSelect.setFetchSize(XLConf.XL_FETCH_SIZE);

			rs = this.pstmtSelect.executeQuery();
			
			Vector<ArrayList<String>> vtData = new Vector<ArrayList<String>>();
			
			String value = null;
			
			// gssg - xl p2p ����
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
					switch ( colInfo.getDataType() ) { // gssg - xl m2m bulk mode ����

					
					// gssg - xl p2p ����
					// gssg - p2p bulk mode Ÿ�� ó��
					// gssg - o2m �ϴٰ� p2p bulk mode ����
					 case XLDicInfoCons.CHAR:
					 case XLDicInfoCons.VARCHAR:					
					 case XLDicInfoCons.VARCHAR2:					
						
						 value = rs.getString(i+1);
						 
						 // gssg - xl p2t ����
						 // gssg - p2t bulk mode Ÿ�� ó��
						 // gssg - xl ��ü������ ����2
						 // gssg - PostgreSQL Ŀ���� ó��
						 if ( tdbType == XLCons.PPAS || tdbType == XLCons.POSTGRESQL ) {
							value = XLUtil.replaceCharToCSV_PPAS(value);							 
						 } else if ( tdbType == XLCons.TIBERO ) {
							 value = XLUtil.replaceCharToCSV_TIBERO(value);
						 }

						break;
						
					// gssg - xl p2p ����
					// gssg - p2p bulk mode Ÿ�� ó��
					 case XLDicInfoCons.TEXT:
					 case XLDicInfoCons.XML:
						 							
						 	value = rs.getString(i+1);				
						 
							value = XLUtil.replaceCharToCSV_PPAS(value);
							
							break;

					// gssg - xl p2p ����
					// gssg - p2p bulk mode Ÿ�� ó��
					 case XLDicInfoCons.BYTEA:
						 
							bAry = rs.getBytes(i+1);
						 
							if (bAry == null) { // gssg - ppas bulk mode null ó��
								value = "\\N";
							} else {
								value = "\\x" + XLUtil.bytesToHexString(bAry);	//  ppas \x �ٿ���
							}
							break;


						
					default : 
						value = rs.getString(i+1);
						
						 // gssg - xl p2t ����
						 // gssg - p2t bulk mode Ÿ�� ó��
						 // gssg - xl ��ü������ ����2
						 // gssg - PostgreSQL Ŀ���� ó��
						if ( value == null ) {							
							// gssg - ��� ����
							// gssg - P2P - \N�� ó��
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
					
					// gssg - ��ü������ ����_start_20221101
					// gssg - potgresql to postgresql bulk mode �ΰ� ó��
					// gssg - ��� ����
					// gssg - P2P - \N�� ó��
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
				
				// gssg - xl m2m bulk mode ����
				if ( tdbType == XLCons.ORACLE && !XLConf.XL_BULK_ORACLE_EOL.equals("")) {
					
					sb_record.append(XLConf.XL_BULK_ORACLE_EOL);
				} else {
					// cksohn - xl bulk mode for oracle - ��� ó�� ���� ����
					// Ÿ���� ����Ŭ�ϰ��� \n write ����
					sb_record.append("\n");
				}
				
				
//				XLLogger.outputInfoLog("CKSOHN DEBUG sb_record = " + sb_record.toString());
				
				
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
			
			// gssg - xl p2t ����
			// gssg - p2t bulk mode ������ ���� ����
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
			
			// gssg - xl p2t ����
			// gssg - p2t bulk mode ������ ���� ����
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

			// gssg - xl ��ü������ ����
		    // gssg - m2m bulk mode thread ���� ����
			// gssg - xl p2p ����
			// gssg - xl p2t ����
			// gssg - p2t bulk mode ������ ���� ����
			// gssg - xl ��ü������ ����2
			// gssg - PostgreSQL Ŀ���� ó��
			// gssg - ��� ����
			// gssg - P2P - bulk ������ ���� ����
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
