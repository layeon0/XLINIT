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
	// 주요 메모리 관리 정보
	// ########################################################
	

	
	// 수행중인 JOB 정보
	//  key : 정책명_JOBSEQ
	//  value : 수행중인 정책정보
	public static Hashtable<String, XLJobRunPol> HT_RUNNING_JOB_INFO = new Hashtable<String, XLJobRunPol>();
	
	
	// XL_DBMS 테이블 정보
	// 	key : ip_dbSid
	// 	value : DBMSInfo
	public static Hashtable<String, XLDBMSInfo> HT_DBMS_INFO = new Hashtable<String, XLDBMSInfo>();
	
	// JOBQ에 들어온 Job들중 소스DBMS기준 수행 대상 DBMS 정보
	// 	key : ip
	// 	value : dbSid -- dbms 정보는 HT_DBMS_INFO 를 참조한다.
	public static Hashtable<String, String> HT_JOBQ_DBMS_TMP = new Hashtable<String, String>();
	
	// cksohn - 서버별 동시 최대작업수 체크 오류 임시수정
	// 	key : sdbip_sdbSid
	// 	value : rCnt
	public static Hashtable<String, Integer> HT_JOBQ_DBMS_RCNT_TMP = new Hashtable<String, Integer>();
	
	
	// HT_JOBQ_POL_CNT
	// 정책 필터링이 끝나고, 수행가능한 정책들의 현재 정책별 Running 중인 Job의 갯수 정보
	//  key : 정책명
	//  value : Running jobCnt
	public static Hashtable<String, Integer> HT_JOBQ_POL_CNT_TMP = new Hashtable<String, Integer>();
	
	// ########################################################
	
	
	// XL_DBMS 정보 메모리 관리
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
				
				// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
				
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
				
				

				// gssg - damo 캐릭터셋 하드 코딩 보완
				// gssg - 캐릭터셋 컬럼 데이터 처리
				String charSet = (String)ht.get("ENCODE_CHAR");				
				String nCharSet = (String)ht.get("ENCODE_NCHAR");

				
				// gssg - damo 캐릭터셋 하드 코딩 보완
				// gssg - 캐릭터셋 컬럼 데이터 처리
				// XLDBMSInfo dbInfo = new XLDBMSInfo(ip, dbSid, port, dbTypeStr, dbType, userId, passwd, cpuThreshold, cpuThresholdTime, maxJobCnt);
				// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
				
					XLLogger.outputInfoLog("[IP]"+ip);
				 // if(ip.equals("192.168.0.197")) {dbSid="soe";}
				  XLDBMSInfo dbInfo = new XLDBMSInfo(ip, dbSid, dbServiceName, port, dbTypeStr, dbType, userId, passwd, charSet, nCharSet);
				  
				  String key = ip + "_" + dbSid;
				  
				  if ( HT_DBMS_INFO.containsKey(key) ) {
				  
				  // 이미 존재하면, 객체를 재생성하지 말고(이미 이 정보를 가지고 구동중인 것들이 있을 수 있으므로), attribute만 변경해준다.
				  HT_DBMS_INFO.get(key).setIp(ip); 
				  HT_DBMS_INFO.get(key).setDbSid(dbSid);
				  
				  // cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
				  HT_DBMS_INFO.get(key).setDbServiceName(dbServiceName);
				  
				  HT_DBMS_INFO.get(key).setPort(port);
				  HT_DBMS_INFO.get(key).setDbTypeStr(dbTypeStr);
				  HT_DBMS_INFO.get(key).setDbType(dbType);
				  HT_DBMS_INFO.get(key).setUserId(userId);
				  HT_DBMS_INFO.get(key).setPasswd(passwd);

				  // gssg - damo 캐릭터셋 하드 코딩 보완 // gssg - 캐릭터셋 컬럼 데이터 처리
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
	
	
	// 현재 수행중인 Running Job의 + or -
	// 파라메터의 _cnt 는 양수 1 또는 음수 -1 이 넘어오므로, 무조건 더해주면 된다. 
	synchronized static public void plusRJobCnt(String _polName, int _cnt)
	{
		try {
			int rcnt = HT_JOBQ_POL_CNT_TMP.get(_polName);

			// 수행 job 갯수 갱신
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
	
	// 수행/완료/중지 JOB 메모리 정보 갱신
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
	
	
	// 실제 수행가고 있는 JOBQ의 정보 갱신
	
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
