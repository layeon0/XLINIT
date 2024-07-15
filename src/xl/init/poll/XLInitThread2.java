package xl.init.poll;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Hashtable;
// ayzn - XLInit ��� ����
import java.util.StringTokenizer;
import java.util.Vector;

import xl.lib.common.XLCons;
import xl.init.conf.XLConf;
import xl.init.dbmgr.XLMDBManager;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLMemInfo;
import xl.init.logger.XLLogger;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

/**
 * 
 * @author ayzn
 * 
 * ���� IDL XLJobPollingThread.java��    XLInitThread.java�� ����
 * IDL���� ����ϴ� JOBQ,�ֱ������� �����ϴ� Polling ���� �ڵ� ���� ��  ����, �α� ����
 *
 */
public class XLInitThread2 extends Thread {

	private String polCode="";
	private String grpCode="";
	private String tableName="";

	public XLInitThread2()
	{
	}
	
	public XLInitThread2(String _grpCode,String _polCode,String _tableName)
	{
		this.grpCode = _grpCode;
		this.polCode = _polCode;
		this.tableName = _tableName;	
	}
	
	@Override
	public void run(){

		XLLogger.outputInfoLog("X-LOG init thread is started.");
		
		Connection cataConn = null;
		
		XLMDBManager mDBMgr = new XLMDBManager();

		if ( XLInit.STOP_FLAG ) {
			XLLogger.outputInfoLog(" init thread thread is stopped - stop request");
			return;
		}
		
		// �޸� �ʱ�ȭ
		XLMemInfo.HT_JOBQ_DBMS_TMP.clear();
		
		// ayzn - XLInit ��� ����  / initThread�߰� : CNT���� �޸� �ּ�ó��
		// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
		//XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.clear();
		
		// ayzn - XLInit ��� ����  / initThread�߰� : CNT���� �޸� �ּ�ó��
		//XLMemInfo.HT_JOBQ_POL_CNT_TMP.clear();

		// ���⼭�� ���� Query�� �����ؾ� �ϹǷ�, �ֱ⸶�� Connection �� �����ϰ� �������� , close �Ѵ�. 
		cataConn = mDBMgr.createConnection(false);
		Vector vt = null;
		try {
			
			// ayzn - XLInit ��� ����  / initThread�߰� : CNT���ø޸� ó�� �ּ�, JOBQ ���� �Լ� �ּ�, CDC īŻ�α� ���� DBó�� ����
			/*
			 
			XLLogger.outputInfoLog("[JOBQ] Polling Check JobQ start");
			
			///// Running ���� JOB ���� �۾� �������� �ð��� ���� �۾����� ���⼭ ABORT
			////// W �۾��� ETIME ���ڰ� �ʰ��� �۾����� CANCEL
			vt = mDBMgr.getJobToCancelOrAbort(cataConn);
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get getJobToCancelOrAbort information.. Retry next Job polling interval time");
				
			} else {
				if ( vt.size() > 1) {
					cancelOrAbortJob(cataConn, vt);
				}
				
			}

			
			// 1. JobQ����  �ҽ� DBMS ���� Running ���� JOB�� ���� ����
			vt = mDBMgr.getRJobCntByDbms(cataConn);
			
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get JobQ information.. Retry next Job polling interval time");
				vt = new Vector(); // JOBQ�� �����Ͱ� �ϳ��� ���� ��ó�� ó���ϱ� ����.
			}
			
			*/
			
			// ayzn - XLInit ��� ���� - 1. �ҽ� DB ���� ����
			String dicOwner = "";
			String dicTname = "";
			
			StringTokenizer tokenizer = new StringTokenizer(this.tableName, ".");
			while(tokenizer.hasMoreTokens()){            
	            if( tokenizer.hasMoreTokens() ) dicOwner = tokenizer.nextToken().trim();
				if( tokenizer.hasMoreTokens() ) dicTname = tokenizer.nextToken().trim();
			} 
			
			XLLogger.outputInfoLog("grpCode = " + this.grpCode);
			XLLogger.outputInfoLog("polCode = " +this.polCode);
			XLLogger.outputInfoLog("SOURCE dicOwner = " + dicOwner);
			XLLogger.outputInfoLog("SOURCE dicTname = " + dicTname);
				
			Vector vt_info = mDBMgr.getSourceInfo(cataConn, this.grpCode, this.polCode, dicOwner, dicTname);
							
			// ayzn - XLInit ��� ����  / initThread�߰� :  size < 1 üũ �߰�
			//if ( vt_info == null) {
			if ( vt_info == null || vt_info.size() < 1){
				XLLogger.outputInfoLog("[WARN] Failed to get information.. ");
				vt_info = new Vector(); // JOBQ�� �����Ͱ� �ϳ��� ���� ��ó�� ó���ϱ� ����.
				System.exit(0);
			}
		
			XLLogger.outputInfoLog(vt_info);
			 
			// ayzn - XLInit ��� ���� : CNT���ø޸� ó�� �ּ�, JOBQ ���� �Լ� �ּ�, CDC īŻ�α� ����  DBó�� ����
			/* 
			
			// 2. dbms_sid��  Running ���� job�� maxJobCnt�� �ʰ����� �ʴ� �͵鸸 1�� ������
			//    - �̶�, Running���� �� ���� job�� �ش� �ҽ� DBMS���� ����Ǵ� ���� �Ϳ� ���� cnt�� ����(0)
			for ( int i=0; i<vt.size(); i++ ) {
				
				Hashtable ht = (Hashtable)vt.get(i);
				String sdbIp = (String)ht.get("JOBQ_SDBIP");
				String sdbSid = (String)ht.get("JOBQ_SDBSID");
				
				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				int rCnt = Integer.parseInt((String)ht.get("RCNT"));
				
				String dbmsKey = sdbIp + "_" + sdbSid;
				// check dbms �� ���� max running job ����
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				if ( dbmsInfo == null ) {
					
					XLLogger.outputInfoLog("[WARN] Cannot find source DBMS infomation - " + sdbIp + "/" + sdbSid);
					XLLogger.outputInfoLog("[WARN] Please Check X-LOG DBMS Information!!!");
					continue;
					
				}
				
				// if  ( rCnt >= dbmsInfo.getMaxJobCnt() ) {
				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				//int rCnt = htRCntDBMS.get(dbmsKey);
				if  ( rCnt >= dbmsInfo.getMaxJobCnt() ) {
					
					XLLogger.outputInfoLog("[JOBQ] Already Running max concurrent job - " + sdbIp + "/" + sdbSid);
					XLLogger.outputInfoLog("       Running Job : " + rCnt + " >= Concurrent Max Job : " + dbmsInfo.getMaxJobCnt()); 
					
				} else {
					
					// JOBQ ���� ��� DBMS ���� ���
					XLMemInfo.HT_JOBQ_DBMS_TMP.put(sdbIp, sdbSid);
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.put(sdbIp + "_" + sdbSid, rCnt);
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					//rCnt++;
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					//htRCntDBMS.put(dbmsKey, rCnt);
				}
				
			} // for-end
			
			*/
			
			// ayzn - XLInit ��� ����  - 2. dbms_sid�� ������ ���
			for ( int i=0; i<vt_info.size(); i++ ) {
				Hashtable ht = (Hashtable)vt_info.get(i);
				
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
			
				String dbmsKey = sdbIp + "_" + sdbSid;
				
				XLLogger.outputInfoLog("[dbmsKey]"+dbmsKey);
				 
				// check dbms �� ���� max running job ����
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				
				if ( dbmsInfo == null ) {
					
					XLLogger.outputInfoLog("[WARN] Cannot find source DBMS infomation - " + sdbIp + "/" + sdbSid);
					XLLogger.outputInfoLog("[WARN] Please Check X-LOG DBMS Information!!!");
					continue;
					
				}
			
				// JOBQ ���� ��� DBMS ���� ���
				XLMemInfo.HT_JOBQ_DBMS_TMP.put(sdbIp, sdbSid);
			} // for-end
			
			// ayzn - XLInit ��� ���� : CNT���ø޸� ó�� �ּ�, JOBQ ���� �Լ� �ּ�
			/*
			 
			// 3.HT_JOBQ_DBMS�� ���డ���� ��å ���� �޸� ���
			//   - ��å�� Runnung ���� �۾���(count : runPolJobCnt �����ؼ� HTRUNNING_POL_CNT �� ���
			//   - runPolJobCnt < tmaxJobCnt �� �͵鸸 �۾����� �����
			vt = mDBMgr.getRJobCntByPol(cataConn);
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get JobQ policy information.. Retry next Job polling interval time");
				Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
				continue;
			}
			
			for ( int i=0; i<vt.size(); i++ ) {
									
				Hashtable ht = (Hashtable)vt.get(i);
				String polName = (String)ht.get("JOBQ_POLNAME");
				int polTMaxJobCnt = Integer.parseInt((String)ht.get("POL_TMAX_JOBCNT"));
				int polRJobCnt = Integer.parseInt((String)ht.get("RCNT"));
				
				if ( polRJobCnt < polTMaxJobCnt ) {
					// �߰� ������ ������ ��å�� ���
					XLMemInfo.HT_JOBQ_POL_CNT_TMP.put(polName, polRJobCnt);						
				}					
				
			} // for-end
			
			*/
			
			// �ش� ��å
			
			// ayzn - XLInit ��� ���� : runJobQ -> runPol�� ���� �� ����ó�� ����
			/*
			 *
			// 4. 3���� HT_JOBQ_POL_CNT_TMP�� ��ϵ� ��� ��å�鿡 ���� ����üũ�� JOB ����
			if ( !runJobQ(cataConn) ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run JobQ ... Retry next Job polling interval time");
				Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
				continue;

			}
			*
			*/
			
			// ayzn - XLInit ��� ���� - 4.��å��� ����
			if ( !runPol(cataConn,vt_info) ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Pol ...");
				
				System.exit(0);
			}	
					
			XLLogger.outputInfoLog("");

			
		} catch(Exception e) {
			
			XLException.outputExceptionLog(e);
			
		} finally {
			
			try { if ( cataConn != null ) cataConn.close(); } catch (Exception e) {} finally { cataConn = null; }
			
		}
	}
	
	// job ����
	private boolean runPol(Connection _cataConn,Vector vt_info)
	{
	
		PreparedStatement pstmt_updateStatus = null;
		
		try {
			
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			// ayzn - XLInit ��� ����  / initThread�߰� : JOBQ ���� �ּ�
			/*
 
			pstmt_updateStatus = mDBMgr.getPstmtUpdateJobQStatus(_cataConn);
			if ( pstmt_updateStatus == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to make updateJobQStatus PreparedStatement");
				return false;
			}
			
			Vector vt = mDBMgr.getJobToRun(_cataConn);
			if ( vt == null ) {
				return false;
			}
			
			if ( vt_info == null ) {
				return false;
			}
			
			*/
			
			
			// ayzn - XLInit ��� ����  / initThread�߰� : source db������  �ʿ��� ���� ����
			for (int i=0; i<vt_info.size(); i++) {
				
				Hashtable ht = (Hashtable)vt_info.get(i);
	
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
				
				String dbmsKey = sdbIp + "_" + sdbSid;
				
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				
				// ���Ǽ��� (nr_clonetb ���̺�)
				String condWhere = (String)ht.get("TB_CONDITION");
				
				XLLogger.outputInfoLog("  condWhere : ["+condWhere +"]");
				XLLogger.outputInfoLog("");
				
				
				XLJobRunPol jobRunPolInfo = new XLJobRunPol(grpCode, polCode, tableName, condWhere);
				
				if ( !jobRunPolInfo.makeInfo(_cataConn) ) {
					XLLogger.outputInfoLog("[WARN] Failed to Run Job policy - " + polCode);
					return false;
				}
				
				XLMemInfo.addRJobPolInfo(polCode, jobRunPolInfo);
				jobRunPolInfo.setsDate(XLUtil.getCurrentDateStr());

				// JOB ����

				// cksohn - BULK mode oracle sqlldr
				// jobRunPolInfo.exeJob();
				// if ( jobRunPolInfo.getExeMode() == XLCons.NORMAL_MODE && jobRunPolInfo.getTdbInfo().getDbType() != XLCons.ORACLE ) {
				// gssg - xl o2p bulk mode ����
				// gssg - ppas bulk thread �߰�
				// gssg - xl t2t ����
				// gssg - t2t bulk mode ����
				if  ( jobRunPolInfo.getTdbInfo().getDbType() == XLCons.ORACLE || 
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.MARIADB || 
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.MYSQL ||
						// gssg - xl ��ü������ ����2
						// gssg - PostgreSQL Ŀ���� ó��
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.PPAS || 
						jobRunPolInfo.getTdbInfo().getDbType() == XLCons.POSTGRESQL ) {
					
					if ( jobRunPolInfo.getExeMode() == XLOGCons.NORMAL_MODE ) { // cksohn - XL_BULK_MODE_YN conf �� ����
						XLLogger.outputInfoLog("[NOMAL MODE]");
						jobRunPolInfo.exeJob();
						
					} else if ( jobRunPolInfo.getExeMode() == XLOGCons.BULK_MODE ) { // BULK MODE - ��������� Ÿ�� Oracle �� ����
						XLLogger.outputInfoLog("[BULK MODE]");
						jobRunPolInfo.exeJobBulk();
						
					} else if ( jobRunPolInfo.getExeMode() == XLOGCons.LINK_MODE ) {
						
						jobRunPolInfo.exeJobLink();
					}
					
				} 
				else {
					
					// BULK ��� ���� ���ϴ� DB�� NORMAL_MODE�� ���� (���� DBMS ���� ���� �ʿ�)
					jobRunPolInfo.exeJob();
					
				}
				
				
			} // for-end
			
			
			return true;
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			return false;
			
		} finally {
			try { if ( pstmt_updateStatus != null ) pstmt_updateStatus.close(); } catch (Exception e) {} finally { pstmt_updateStatus = null; }
		}
	}
	

}
