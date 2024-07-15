package xl.init.info;

import java.util.Hashtable;
import java.util.Vector;

import xl.lib.common.XLEncryptor;
import xl.init.conf.XLConf;
import xl.init.dbmgr.XLMDBManager;
import xl.init.logger.XLLogger;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

public class XLMemInfo {

	
	// ########################################################
	// �ֿ� �޸� ���� ����
	// ########################################################
	

	
	// �������� JOB ����
	//  key : ��å��_JOBSEQ
	//  value : �������� ��å����
	public static Hashtable<String, XLJobRunPol> HT_RUNNING_JOB_INFO = new Hashtable<String, XLJobRunPol>();
	
	
	// XL_DBMS ���̺� ����
	// 	key : ip_dbSid
	// 	value : DBMSInfo
	public static Hashtable<String, XLDBMSInfo> HT_DBMS_INFO = new Hashtable<String, XLDBMSInfo>();
	
	// JOBQ�� ���� Job���� �ҽ�DBMS���� ���� ��� DBMS ����
	// 	key : ip
	// 	value : dbSid -- dbms ������ HT_DBMS_INFO �� �����Ѵ�.
	public static Hashtable<String, String> HT_JOBQ_DBMS_TMP = new Hashtable<String, String>();
	
	// cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü���
	// 	key : sdbip_sdbSid
	// 	value : rCnt
	public static Hashtable<String, Integer> HT_JOBQ_DBMS_RCNT_TMP = new Hashtable<String, Integer>();
	
	
	// HT_JOBQ_POL_CNT
	// ��å ���͸��� ������, ���డ���� ��å���� ���� ��å�� Running ���� Job�� ���� ����
	//  key : ��å��
	//  value : Running jobCnt
	public static Hashtable<String, Integer> HT_JOBQ_POL_CNT_TMP = new Hashtable<String, Integer>();
	
	// ########################################################
	
	
	// XL_DBMS ���� �޸� ����
	synchronized public static boolean registDbmsInfo()
	{
		try {
					
			XLMDBManager mDBMgr = new XLMDBManager();
			Vector vt = mDBMgr.getDbmsInfo();
					
			while ( vt == null ) {
				XLLogger.outputInfoLog("Retry after 3 secs...");
				Thread.sleep(3000);
				
				vt = mDBMgr.getDbmsInfo();
			}
			
			for (int i=0; i<vt.size(); i++) {
				
				Hashtable ht = (Hashtable)vt.get(i);
				
				String ip = (String)ht.get("SVR_IPADDR");
				String dbSid = (String)ht.get("DBMS_SID");
				
				// cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���
				
				String dbServiceName = (String)ht.get("DBMS_SID");
				
				int port = Integer.parseInt((String)ht.get("DBMS_PORT"));
				
				String dbTypeStr = (String)ht.get("DBMS_TYPE");
				byte dbType = XLUtil.getDBMSType(dbTypeStr);
				
				String userId = (String)ht.get("DBMS_USERID");
				
				String passwd = (String)ht.get("DBMS_PASSWD"); // ENCRYPT		
								
				if ( XLConf.XL_PASSWD_ENCRYPT_YN ) {
					XLEncryptor xlogEncryptor = new XLEncryptor();
					passwd	= xlogEncryptor.decrypt(passwd); 
				} 
				
				

				// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
				// gssg - ĳ���ͼ� �÷� ������ ó��
				String charSet = (String)ht.get("ENCODE_CHAR");				
				String nCharSet = (String)ht.get("ENCODE_NCHAR");

				
				// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
				// gssg - ĳ���ͼ� �÷� ������ ó��
				// XLDBMSInfo dbInfo = new XLDBMSInfo(ip, dbSid, port, dbTypeStr, dbType, userId, passwd, cpuThreshold, cpuThresholdTime, maxJobCnt);
				// cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���
				
					XLLogger.outputInfoLog("[IP]"+ip);
				 // if(ip.equals("192.168.0.197")) {dbSid="soe";}
				  XLDBMSInfo dbInfo = new XLDBMSInfo(ip, dbSid, dbServiceName, port, dbTypeStr, dbType, userId, passwd, charSet, nCharSet);
				  
				  String key = ip + "_" + dbSid;
				  
				  if ( HT_DBMS_INFO.containsKey(key) ) {
				  
				  // �̹� �����ϸ�, ��ü�� ��������� ����(�̹� �� ������ ������ �������� �͵��� ���� �� �����Ƿ�), attribute�� �������ش�.
				  HT_DBMS_INFO.get(key).setIp(ip); 
				  HT_DBMS_INFO.get(key).setDbSid(dbSid);
				  
				  // cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���
				  HT_DBMS_INFO.get(key).setDbServiceName(dbServiceName);
				  
				  HT_DBMS_INFO.get(key).setPort(port);
				  HT_DBMS_INFO.get(key).setDbTypeStr(dbTypeStr);
				  HT_DBMS_INFO.get(key).setDbType(dbType);
				  HT_DBMS_INFO.get(key).setUserId(userId);
				  HT_DBMS_INFO.get(key).setPasswd(passwd);

				  // gssg - damo ĳ���ͼ� �ϵ� �ڵ� ���� // gssg - ĳ���ͼ� �÷� ������ ó��
				  HT_DBMS_INFO.get(key).setCharSet(charSet);
				  HT_DBMS_INFO.get(key).setnCharSet(nCharSet);
				  
				  XLLogger.outputInfoLog("[DBMS] " + ip + " / " + dbSid + " is updated");
				  
				  } else {
				  
				  HT_DBMS_INFO.put(key, dbInfo); XLLogger.outputInfoLog("[DBMS] " + ip + " / "
				  + dbSid + " is registered");
				  
				  }
			}
		
			return true;
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
			XLLogger.outputInfoLog("[EXCEPTION] Failed to get dbms info.");
			return false;
			
		}
		
	}
	
	
	// ���� �������� Running Job�� + or -
	// �Ķ������ _cnt �� ��� 1 �Ǵ� ���� -1 �� �Ѿ���Ƿ�, ������ �����ָ� �ȴ�. 
	synchronized static public void plusRJobCnt(String _polName, int _cnt)
	{
		try {
			int rcnt = HT_JOBQ_POL_CNT_TMP.get(_polName);

			// ���� job ���� ����
			rcnt = rcnt + (_cnt);
			HT_JOBQ_POL_CNT_TMP.put(_polName, rcnt);
			
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
		}
			
	}
	
	
	synchronized static public int getRJobCnt(String _polName)
	{
		try {
			return HT_JOBQ_POL_CNT_TMP.get(_polName);

		} catch (Exception e) {
			// XLMgrException.outputExceptionLog(e);
			XLLogger.outputInfoLog("[EXCEPTION] getRJobCnt - " + e.toString());
			return -1;
		}
			
	}
	
	// ����/�Ϸ�/���� JOB �޸� ���� ����
	// _type
	//	
	synchronized static public void removeRJobCntInfo(String _polName)
	{
		try {
			
			HT_JOBQ_POL_CNT_TMP.remove(_polName);
			
			
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
		}
	}
	
	
	// ���� ���డ�� �ִ� JOBQ�� ���� ����
	
	  synchronized static public boolean addRJobPolInfo(String _polName,  XLJobRunPol _jobRunPolInfo) { try {
	  
	  String key = _polName;
	  
	  HT_RUNNING_JOB_INFO.put(key, _jobRunPolInfo);
	  
	  return true; } catch (Exception e) { // XLMgrException.outputExceptionLog(e);
	  XLLogger.outputInfoLog("[EXCEPTION] updateRJobPolInfo - " + e.toString());
	  return false; }
	  
	  }
	 
	
	
	 synchronized static public boolean removeRJobPolInfo(String _polName) { try {
	  
	  String key = _polName;
	  
	  HT_RUNNING_JOB_INFO.remove(key);
	  
	  return true; } catch (Exception e) { // XLMgrException.outputExceptionLog(e);
	  XLLogger.outputInfoLog("[EXCEPTION] updateRJobPolInfo - " + e.toString());
	  return false; }
	  
	  }
	 
}
