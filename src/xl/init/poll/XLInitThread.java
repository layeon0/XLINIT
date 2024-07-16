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
public class XLInitThread extends Thread {
	
	// ayzn - XLInit ��� ����  - InitThread : �޾ƿ��� �ɼ� �� ����(��å��, �׷��, ���̺��)
	private String polCode="";
	private String grpCode="";
	private String tableName="";

	public XLInitThread()
	{
	}
	
	// ayzn - XLInit ��� ����  - InitThread : ������ �߰�
	public XLInitThread(String _grpCode,String _polCode,String _tableName)
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
		
		// ayzn - XLInit ��� ����  - InitThread : ����  polling ����� while �ּ�ó��
		//while ( true ) {
		if ( XLInit.STOP_FLAG ) {
			XLLogger.outputInfoLog(" init thread thread is stopped - stop request");
			return;
		}
		
		// �޸� �ʱ�ȭ
		XLMemInfo.HT_JOBQ_DBMS_TMP.clear();
		
		// ayzn - XLInit ��� ����  - InitThread : CNT���� �޸� �ּ�
		// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
		/*XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.clear();
		
		XLMemInfo.HT_JOBQ_POL_CNT_TMP.clear();*/

		// ���⼭�� ���� Query�� �����ؾ� �ϹǷ�, �ֱ⸶�� Connection �� �����ϰ� �������� , close �Ѵ�. 
		cataConn = mDBMgr.createConnection(false);
		Vector vt = null;
		try {
			
			// ayzn - XLInit ��� ����  - InitThread : IDL�� JOBQ ���̺�ó�� �ּ�
			///// Running ���� JOB ���� �۾� �������� �ð��� ���� �۾����� ���⼭ ABORT
			////// W �۾��� ETIME ���ڰ� �ʰ��� �۾����� CANCEL
			/*vt = mDBMgr.getJobToCancelOrAbort(cataConn);
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to get getJobToCancelOrAbort information.. Retry next Job polling interval time");
				
			} else {
				if ( vt.size() > 1) {
					cancelOrAbortJob(cataConn, vt);
				}
				
			}
		
			 */
					
			// ayzn - XLInit ��� ����  - InitThread : 1. �ҽ� DB ���� ����
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
			
			// ayzn - XLInit ��� ����  - InitThread : JOBQ -> SOURCE DB ���� ����
			// 1. JobQ����  �ҽ� DBMS ���� Running ���� JOB�� ���� ����
			//vt = mDBMgr.getRJobCntByDbms(cataConn);
			Vector vt_info = mDBMgr.getSourceInfo(cataConn, this.grpCode, this.polCode, dicOwner, dicTname);
							
			// ayzn - XLInit ��� ����  - InitThread :  size < 1 üũ �߰�
			//if ( vt_info == null) {
			if ( vt_info == null || vt_info.size() < 1){
				XLLogger.outputInfoLog("[WARN] Failed to get information.. ");
				vt_info = new Vector(); // JOBQ�� �����Ͱ� �ϳ��� ���� ��ó�� ó���ϱ� ����.
				System.exit(0);
			}
		
			XLLogger.outputInfoLog(vt_info);
			 
			///// TODO Running ���� JOB ���� �۾� �������� �ð��� ���� �۾����� ���⼭ 
			///// ���� ����ñ��� continue;
					
					
			// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
			/**
			Hashtable<String, Integer> htRCntDBMS = new Hashtable<String, Integer>();
			for ( int i=0; i<vt.size(); i++ ) {
				Hashtable ht = (Hashtable)vt.get(i);
				String sdbIp_tmp = (String)ht.get("JOBQ_SDBIP");
				String sdbSid_tmp = (String)ht.get("JOBQ_SDBSID");
				String dbmsKey_tmp = sdbIp_tmp + "_" + sdbSid_tmp;
				
				htRCntDBMS.put(dbmsKey_tmp, Integer.parseInt((String)ht.get("RCNT")) );
			}
			**/
			
			// ayzn - XLInit ��� ����  - InitThread : JOBQ -> SOURCE DB ���� ���� �� HT_JOBQ_DBMS_RCNT_TMP �ּ�
			
			// 2. dbms_sid��  Running ���� job�� maxJobCnt�� �ʰ����� �ʴ� �͵鸸 1�� ������
			//    - �̶�, Running���� �� ���� job�� �ش� �ҽ� DBMS���� ����Ǵ� ���� �Ϳ� ���� cnt�� ����(0)
			for ( int i=0; i<vt_info.size(); i++ ) {
					
				Hashtable ht = (Hashtable)vt_info.get(i);
				
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
				
				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				//int rCnt = Integer.parseInt((String)ht.get("RCNT"));
				
				String dbmsKey = sdbIp + "_" + sdbSid;
				XLLogger.outputInfoLog("[dbmsKey]"+dbmsKey);
				
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
				//if  ( rCnt >= dbmsInfo.getMaxJobCnt() ) {
					
				//	XLLogger.outputInfoLog("[JOBQ] Already Running max concurrent job - " + sdbIp + "/" + sdbSid);
				//	XLLogger.outputInfoLog("       Running Job : " + rCnt + " >= Concurrent Max Job : " + dbmsInfo.getMaxJobCnt()); 
					
				//} else {
					
					// JOBQ ���� ��� DBMS ���� ���
					XLMemInfo.HT_JOBQ_DBMS_TMP.put(sdbIp, sdbSid);
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					//XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.put(sdbIp + "_" + sdbSid, rCnt);
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					//rCnt++;
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					//htRCntDBMS.put(dbmsKey, rCnt);
				//}
					
			} // for-end
			
			
			// 3.HT_JOBQ_DBMS�� ���డ���� ��å ���� �޸� ���
			//   - ��å�� Runnung ���� �۾���(count : runPolJobCnt �����ؼ� HTRUNNING_POL_CNT �� ���
			//   - runPolJobCnt < tmaxJobCnt �� �͵鸸 �۾����� �����
			// ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ�  �ڵ� �ּ�
			/*vt = mDBMgr.getRJobCntByPol(cataConn);
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
				
			} // for-end*/
			
			
			
			// �ش� ��å
			
			// 4. 3���� HT_JOBQ_POL_CNT_TMP�� ��ϵ� ��� ��å�鿡 ���� ����üũ�� JOB ����
			// ayzn - XLInit ��� ����  - InitThread : �Լ��� runPol�� ���� �� continue���� �ý�������� ����
			//if ( !runJobQ(cataConn) ) {
			if ( !runPol(cataConn,vt_info) ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Pol ...");
				Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
				System.exit(0);

			}	
					
			XLLogger.outputInfoLog("");

			XLInit.POLLING_EVENTQ.waitEvent();
			
			
			
			// Thread.sleep(XLConf.XL_MGR_POLLING_INT*1000);
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
			
			// ���డ���� ��å�鿡 ����(HT_JOBQ_POL_CNT) ���� �������� üũ�ϸ鼭 ����
			// ���� ��������
			// ��å�� ���� �ִ��۾��� < ��å�� ���� �������� �۾� �ΰ͵� �� ���� 
			XLMDBManager mDBMgr = new XLMDBManager();
			// ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ�  JOPQ ���� �ڵ� �ּ�
			/*pstmt_updateStatus = mDBMgr.getPstmtUpdateJobQStatus(_cataConn);
			if ( pstmt_updateStatus == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to make updateJobQStatus PreparedStatement");
				return false;
			}
			
			Vector vt = mDBMgr.getJobToRun(_cataConn);
			if ( vt == null ) {
				return false;
			}*/
			
			
			// ayzn - XLInit ��� ����  - InitThread : source db������  �ʿ��� ���� ����
			for (int i=0; i<vt_info.size(); i++) {
				
				Hashtable ht = (Hashtable)vt_info.get(i);
				
				/*String polName = (String)ht.get("JOBQ_POLNAME");				
				if (  !XLMemInfo.HT_JOBQ_POL_CNT_TMP.containsKey(polName) ) {
					// maxJobCnt�� �ʰ��Ǿ� �����Ǿ��ų�, �������� �ƴ� ��å skip
					continue;
				}*/
				
				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				String sdbIp  = (String)ht.get("DBMS_IP");
				String sdbSid = (String)ht.get("DBMS_SID");
				String dbmsKey = sdbIp + "_" + sdbSid;
				
				XLDBMSInfo dbmsInfo = XLMemInfo.HT_DBMS_INFO.get(dbmsKey);
				//int rCnt_DBMS = XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.get(dbmsKey);
				
				//XLLogger.outputInfoLog("DEBUG rCnt_DBMS/MAX_DBMS_JOB = " + rCnt_DBMS + "/" + dbmsInfo.getMaxJobCnt());
				
				//if (  rCnt_DBMS >=  dbmsInfo.getMaxJobCnt() ) {
					// dbms �� maxJobCnt�� �ʰ��Ǿ� �����Ǿ��ų�, �������� �ƴ� ��å skip
					//continue;
				//}
				
				// ��å ����
				// 1. ��å ���� ���� ���� JobRunPolInfo
				// ayzn - XLInit ��� ����  - InitThread : condWhere���� �ּ�ó��
				//long jobSeq = Long.parseLong((String)ht.get("JOBQ_SEQ"));
				String condWhere = (String)ht.get("TB_CONDITION");
				//int tmaxCnt = Integer.parseInt((String)ht.get("POL_TMAX_JOBCNT"));
				
				XLLogger.outputInfoLog("  condWhere : ["+condWhere +"]");
				XLLogger.outputInfoLog("");
				
				// ayzn - XLInit ��� ����  - InitThread : ���ڰ� ����
				//XLJobRunPol jobRunPolInfo = new XLJobRunPol(jobSeq, polName, condWhere);
				XLJobRunPol jobRunPolInfo = new XLJobRunPol(grpCode, polCode, tableName, condWhere);
				if ( !jobRunPolInfo.makeInfo(_cataConn) ) {
					XLLogger.outputInfoLog("[WARN] Failed to Run Job policy - " + polCode);
					return false;
				}
				
				// ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ�  cnt�޸� ���� �ڵ� �ּ�
				// �ش� ��å�� running count ���� (-1�� ����)
				/*XLMemInfo.plusRJobCnt(polName, 1); 
				
				if ( XLMemInfo.getRJobCnt(polName) >= tmaxCnt ) {
					// ���Ŀ��� �����ϸ� �ȵǹǷ�, �� ��å ������ ����
					// �����Ǿ����Ƿ�, �� ��å�� JOB��  skip��.
					XLMemInfo.removeRJobCntInfo(polName);
					
					// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
					// !!!!! continue???
					//continue;
					
				}*/
				
				// ayzn - XLInit ��� ����  - InitThread : jobseq�����ϰ� ó��
				// �ش� ��å�� running job �޸𸮿� ���
				//XLMemInfo.addRJobPolInfo(polName, jobSeq, jobRunPolInfo);
				XLMemInfo.addRJobPolInfo(polCode, jobRunPolInfo);
				
				// ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ� �ڵ� �ּ�
				// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
				// DBMS rCNT ����
				//XLMemInfo.HT_JOBQ_DBMS_RCNT_TMP.put(dbmsKey, ++rCnt_DBMS);
				
				jobRunPolInfo.setsDate(XLUtil.getCurrentDateStr());
				
				// ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ� �ڵ� �ּ�
				/*
				// JOB STATUS update W-->R
				pstmt_updateStatus.setString(1, "R");
				
				// pstmt_updateStatus.setString(2, jobRunPolInfo.getsDate());
				// setTimestamp(_setCnt, Timestamp.valueOf(_value));
				// cksohn - catalog ppas
				// cksohn - SCHED_ETIME üũ ���� ����
				
				if ( jobRunPolInfo.getsDate() == null || jobRunPolInfo.getsDate().equals("") ) {
					pstmt_updateStatus.setNull(2, java.sql.Types.NULL);
				} else {
					pstmt_updateStatus.setTimestamp(2, Timestamp.valueOf(jobRunPolInfo.getsDate()) );	
				}
				
				pstmt_updateStatus.setLong(3, jobSeq);
				pstmt_updateStatus.setString(4, polName);
				pstmt_updateStatus.executeUpdate();
				_cataConn.commit(); // �̰� �Ǵ����� commit
				*/
				
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
	
	
	// Cancel or Abort
	/*private void cancelOrAbortJob(Connection _cataConn, Vector _vtJob)
	{
				
		try {
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			XLLogger.outputInfoLog("#####  CALCEL OR ABORT JOB PROCESSING #####");
			
			
			for ( int i=0; i<_vtJob.size(); i++) {
				
				Hashtable ht = (Hashtable)_vtJob.get(i);
				
				long jobSeq = Long.parseLong((String)ht.get("JOBQ_SEQ"));
				String polName = (String)ht.get("JOBQ_POLNAME");
				String condWhere = (String)ht.get("JOBQ_CONDITION_WHERE");
				
				String jobQ_status = (String)ht.get("JOBQ_STATUS");
				
				// JobQ Jobq ���� ���� ����
				XLJobRunPol jobRunPol = new XLJobRunPol(jobSeq, polName, condWhere);
				if ( !jobRunPol.makeInfo(_cataConn) ) {
					XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to make JobQ information - " + polName);
					continue;
				}
				
				// if ( jobQ_status.equals(XLCons.STATUS_CANCEL) ) {
				// cksohn - SCHED_ETIME üũ ���� ����
				if ( jobQ_status.equals(XLOGCons.STATUS_WAIT) ) {
					
				
					jobRunPol.setJobStatus(XLOGCons.STATUS_CANCEL);
					jobRunPol.setErrMsg_Apply("CANCEL Waiting job because of Job start end time over.");
					
					// 2. status �� ���� �������� �� REPORT ��� ����
					//  2-1 REPORT ���̺� �������
					if ( !mDBMgr.insertJobResultReport(_cataConn, jobRunPol, jobRunPol.getCondCommitCnt()) ) {
						XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to insert job result report - " + jobRunPol.getCondWhere());
					}
					
					//  2-2 CONDITION ���̺� STATUS update
					if ( !mDBMgr.updateJobResultCond(_cataConn, jobRunPol) ) {
						XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to update job result condition_action - " + jobRunPol.getCondWhere());
					}
								
					//  2-3 JOBQ ���̺� ����
					if ( !mDBMgr.deleteJobQ(_cataConn, jobRunPol) ) {
						XLLogger.outputInfoLog("[WARN][cancelOrAbortJob] Failed to delete jobQ - " + jobRunPol.getCondWhere());
					}
					
					XLLogger.outputInfoLog("[CANCEL JOB][cancelOrAbortJob] " + jobSeq + " / " + polName);
					
				} else { // ABORT JOB Processing
					
					
					String key = polName + "_" + jobSeq;
					if ( XLMemInfo.HT_RUNNING_JOB_INFO.containsKey(key) ) {
						XLLogger.outputInfoLog("[ABORT JOB][cancelOrAbortJob] " + jobSeq + " / " + polName + " Request. - Abort next polling time");
						
						try { 
							XLJobRunPol jobPolInfo = XLMemInfo.HT_RUNNING_JOB_INFO.get(key);
							jobPolInfo.setErrMsg_Recv("ABORT Running job because of Job execution end time over.");
							jobPolInfo.setStopJobFlag(true);
							
						} catch (Exception ee) {
							XLException.outputExceptionLog(ee);
							
						}
						
					} else {
						XLLogger.outputInfoLog("[ABORT JOB][cancelOrAbortJob] Cannot find Running Job Info in Memory - " + jobSeq + " / " + polName );
					}
					
				}
				
			} // for-end
			
			_cataConn.commit();

			XLLogger.outputInfoLog("###########################################");

			 
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
		} finally {

		}
		
	}*/
}
