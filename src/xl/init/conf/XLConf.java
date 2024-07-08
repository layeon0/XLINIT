package xl.init.conf;
// import org.w3c.dom.*;
import java.io.BufferedReader;


import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

import xl.lib.common.XLCons;
import xl.lib.common.XLEncryptor;
import xl.init.dbmgr.XLMDBManager;
import xl.init.logger.XLLogger;
import xl.init.main.XLInit;
import xl.init.util.XLUtil;


public class XLConf
{
	
	// log config
	public static String 	XL_CONFIG;
	public static String 	XL_LOG_FILE = "/opt/XL/XLInit/log";
	public static String 	XL_LOG_FILE_NAME = "xlinit.log";
	
	
	// ---------------------------------------------------
	// Catalog DB Properties from xl.conf
	// ---------------------------------------------------
	
	public static byte		DBTYPE_SRC = XLCons.ORACLE;
	public static byte 		DBTYPE_STR = XLCons.ORACLE;
	public static String 	DB_IP = "localhost";
	public static int 		DB_PORT = 1521;
	public static String 	DB_ID = "xladmin";
	public static String 	DB_PASS = "xladmin";
	public static String 	DB_SID = "XLOGDB";
	// cksohn - catalog db passwd 암호화 옵션 - XL_PASSWD_ENCRYPT_YN ( default Y) 
	public static boolean 	XL_PASSWD_ENCRYPT_YN = true;	
	// cksohn - XL_PASSWD_CONF_ENCRYPT_YN (default N)
	public static boolean 	XL_PASSWD_CONF_ENCRYPT_YN = false;
	

	public static String    BULK_MODE = XLInit.bulkMode;
	
	// ---------------------------------------------------

	
	//---------------------------------------------------
	// from catalog db - XL_CONF
	//---------------------------------------------------
	public static String 	XL_LOG_CONF = "30,10";
	public static int 		XL_LOG_LEVEL = 2;
	
	// mgr process port
	public static int 		XL_MGR_PORT = 9050;

	public static int 		XL_MGR_POLLING_INT = 3600; // catalog db polling interval (sec)
	
	// 엔진 수행시 Recv와 Apply 간 데이터를 주고 받는 내부Q size
	public static int 		XL_MGR_INTERNAL_QSIZE = 2;
	
	public static boolean 	XL_DEBUG_YN = true;
	
	public static int 		XL_SOCK_TIMEOUT = 1000;  // sec
		
	// db 재접속 시도 횟수
	public static int 		XL_DBCON_RETRYCNT = 100;
	
	
	// cksohn - manager 1.0 최초 버전 수정
	public static int 		XL_FETCH_SIZE = 2000;
	public static int 		XL_BATCH_SIZE = 2000;
	public static int 		XL_COMMIT_COUNT = 100000;
	public static int		XL_PARALLEL = 1;
	public static int 		XL_MGR_SEND_COUNT = 3000;
	
	// cksohn - xlim LOB 타입 지원
	public static int 		XL_APPLY_LOB_STREAM_BUFFERSIZE = 32768;
	
	// cksohn - XL_BULK_MODE_YN conf 값 설정
	//public static boolean 	XL_BULK_MODE_YN = false;
	public static boolean 	XL_BULK_MODE_YN = false;
	
	
	// cksohn - XL_INIT_SCN
	public static long 		XL_INIT_SCN = 0;
	
	
	// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE
	public static boolean 	XL_SRC_CHAR_RAWTOHEX_YN = false;
	public static String 	XL_SRC_CHAR_ENCODE = "MS949";
	
	
	// cksohn - xl XL_LOB_STREAM_YN=Y|*N
	public static boolean 	XL_LOB_STREAM_YN = false;
	
	// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
	// Default = "$XnL!" --> HexString = 24586E4C21
	// "STR X'24586E4C21'" 와 같은 ctl 파일 포맷으로 변경해서 설정
	public static String 	XL_BULK_ORACLE_EOL = "$XnL!";
	public static String    XL_BULK_ORACLE_EOL_CTL_FORMAT = "\"STR X'24586E4C21'\"";

	
	// gssg - 전체적으로 보완_start_20221101
	// gssg - m2m TIMEZONE 설정 추가
	// gssg - LG엔솔 MS2O
	// gssg - o2o bulk ltz 보완
	public static String	XL_TIMEZONE = "";
	
	
	// gssg - o2o damo 적용
	// gssg - XL_TAR_KEYFILE_PATH conf 값 추가
	public static String XL_TAR_KEYFILE_PATH = "";
	
	// gssg - csv file create 기능 추가
	// gssg - conf 값 추가
	public static String XL_CREATE_FILE_PATH = XLInit.XL_DIR + File.separator + "csv";
				
	public static boolean  XL_CREATE_FILE_YN = false;
	
	public static String XL_CREATE_FILE_DELIMITER = "|";
	
	public static String XL_CREATE_FILE_EXTENSION = "dat";
	
	// gssg - LG엔솔 MS2O
	// gssg - ora1400 스킵 처리
	public static boolean XL_ORA1400_SKIP_YN = false;
	
	// gssg - postgresql 커넥션 타임 아웃 보완
	public static int XL_CHK_POSTGRESQL_IDLE_SESSION_TIMEOUT = 0;
	
	// gssg - SK실트론 O2O
	// gssg - alias 지원
	public static String XL_MGR_ALIAS = "";
	
	// gssg - 대법원 O2O
	// gssg - raw_to_varchar2 기능 지원
	public static boolean XL_CHAR_FUNCAPPLY = false;
	
	// gssg - 일본 네트워크 분산 처리
	public static String XL_WORKPLAN_NAME = "";
	
	// gssg - 세븐일레븐 O2MS
	public static boolean XL_MGR_DATA_DEBUG_YN = false;	
		
	//---------------------------------------------------
	
	public XLConf()
	{
	}
	
	public static boolean initConf()
	{
		try {
			
			
			
			//###########################
			// 1. xl.conf read
			//###########################
			//---------------------------------------------------batch_size
			// sets LOG FILE NAMES
			//--------------------------------------------------- 
			XL_CONFIG = XLInit.XL_DIR + File.separator + "conf" + File.separator + "xl.conf";
			//---------------------------------------- -----------
			// sets a log4j
			//---------------------------------------------------
			
			//XLLogger.outputInfoLog("");
			
			if(XLInit.commit_count!=null && XLInit.commit_count > 0)
			{
				XL_COMMIT_COUNT = XLInit.commit_count;
			}
			
			if(XLInit.parallel!=null && XLInit.parallel > 0)
			{
				XL_PARALLEL = XLInit.parallel;
			}
			
			if(XLInit.batch_size!=null && XLInit.batch_size > 0)
			{
				XL_BATCH_SIZE = XLInit.batch_size;
			}
			
			if(XLInit.fetch_size!=null && XLInit.fetch_size > 0)
			{
				XL_FETCH_SIZE = XLInit.fetch_size;
			}
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			Date now = new Date();
			String nowTime = sdf.format(now);
			
	        XL_LOG_FILE = XLInit.XL_DIR + File.separator + "log";
	        //XL_LOG_FILE_NAME = "xlinit.log";
	        
	        //로그파일형식 오늘날짜시분초_XLINIT_G01_P101_TESTUSER.XL_TEST01.LOG
	        XL_LOG_FILE_NAME = nowTime + "_XLINIT_" + XLInit.grpCode + "_" + XLInit.polCode + "_" + XLInit.tableName + ".log";
	        
	        XLLogger.init(XL_LOG_FILE, XL_LOG_FILE_NAME);
	        
	        XLLogger.outputInfoLog("X-LOG Configuration set starting...");
	       
	        XLLogger.outputInfoLog("Init conf start -[");
	        //---------------------------------------------------
			// sets a NR Master
			//---------------------------------------------------	
			String dbtype_str = XLConf.getConfValue( "NRM_DBTYPE_STR" );
			DBTYPE_STR = XLUtil.getDBMSType(dbtype_str);
			
			// gssg -xl lob 타입 보완
			// gssg - LOB 컬럼 순서 끝으로 처리
			String dbtype_src = XLConf.getConfValue( "NRM_DBTYPE_SRC" );
			DBTYPE_SRC = XLUtil.getDBMSType(dbtype_src);
			
			String db_ip = XLConf.getConfValue( "NRM_DB_IP" );   
			if( !db_ip.equals("") ) DB_IP = db_ip;
			String db_port = XLConf.getConfValue( "NRM_DB_PORT" );   
			if( !db_port.equals("") ) DB_PORT = Integer.parseInt(db_port);
			String db_id = XLConf.getConfValue( "NRM_DB_ID" );   
			if( !db_id.equals("") ) DB_ID = db_id;
			
		
			//String db_pass = NRAPConf.getConfValue("NRM_DB_PASS");
			// if (!db_pass.equals(""))
			//	NRM_DB_PASS = db_pass;			
			// cksohn - XL_PASSWD_CONF_ENCRYPT_YN
			String passwd_conf_encrypt_yn = XLConf.getConfValue("XL_PASSWD_CONF_ENCRYPT_YN");
			if (passwd_conf_encrypt_yn.equalsIgnoreCase("Y"))
				XL_PASSWD_CONF_ENCRYPT_YN = true;
			else
				XL_PASSWD_CONF_ENCRYPT_YN = false; // default
			
			// cksohn - XL_PASSWD_CONF_ENCRYPT_YN
			String db_pass = XLConf.getConfValue("NRM_DB_PASS");
			
			// System.out.println("CKSOHN DEBUG----- db_pass = " + db_pass);
			
			if ( XL_PASSWD_CONF_ENCRYPT_YN ) {
				XLEncryptor xlogEncryptor = new XLEncryptor();
				DB_PASS	= xlogEncryptor.decrypt(db_pass); 
			} else {
				DB_PASS = db_pass;
			}			
			
			String db_Sid = XLConf.getConfValue( "NRM_DB_SID" );   
			if( !db_Sid.equals("") ) DB_SID = db_Sid;

			
			// cksohn - catalog db passwd 암호화 옵션 -- SCHED에서는 NOT USED
			String passwd_encrypt_yn = XLConf.getConfValue( "XL_PASSWD_ENCRYPT_YN" );   
			if( passwd_encrypt_yn.equalsIgnoreCase("N") ) XL_PASSWD_ENCRYPT_YN = false;
			else XL_PASSWD_ENCRYPT_YN = true; // default
			
			//###########################
			// 2. catalog db conf read
			//###########################
//			XLLogger.outputInfoLog("CKSOHN DEBUG ----------------!!!");			
//			long stime = System.currentTimeMillis();
					
			//lay : IDL은 conf 관련 테이블이 있으나, CDC는 없음
			//Vector vt = mDBMgr.getConfValues();		
			
			//lay : set bulkmoad
			if(BULK_MODE.equals("Y") || BULK_MODE.equals("y"))
			{
				XL_BULK_MODE_YN = true;
			}
			
//			long etime = System.currentTimeMillis();
//			XLLogger.outputInfoLog("CKSOHN DEBUG Elapsed : " + (etime-stime));
			
			int retryCnt = 0;
			
			XLLogger.outputInfoLog("] Init conf - end");
			//System.out.println("CKSOHN DEBUG-------- initConf end ");
			
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	// cksohn - scheduler 1.0 최초 버전 수정
	public static void writeLogConf()
	{	
		XLLogger.outputInfoLog("[CONF] DB_IP = " + DB_IP);
		XLLogger.outputInfoLog("[CONF] DB_PORT = " + DB_PORT);
		XLLogger.outputInfoLog("[CONF] DB_SID = " + DB_SID);
		XLLogger.outputInfoLog("[CONF] XL_PASSWD_ENCRYPT_YN = " + XL_PASSWD_ENCRYPT_YN);
		XLLogger.outputInfoLog("[CONF] XL_PASSWD_CONF_ENCRYPT_YN = " + XL_PASSWD_CONF_ENCRYPT_YN);
		XLLogger.outputInfoLog("[CONF] XL_LOG_CONF = " + XL_LOG_CONF );
		XLLogger.outputInfoLog("[CONF] XL_LOG_LEVEL = " + XL_LOG_LEVEL  );
		XLLogger.outputInfoLog("[CONF] XL_DBCON_RETRYCNT  = " + XL_DBCON_RETRYCNT);
		XLLogger.outputInfoLog("");
		XLLogger.outputInfoLog("[CONF] XL_COMMIT_COUNT  = " + XL_COMMIT_COUNT);
		XLLogger.outputInfoLog("[CONF] XL_PARALLEL  = " + XL_PARALLEL);
		XLLogger.outputInfoLog("[CONF] XL_BATCH_SIZE  = " + XL_BATCH_SIZE);
		XLLogger.outputInfoLog("[CONF] XL_FETCH_SIZE  = " + XL_FETCH_SIZE);
		
		
		
		// cksohn - XL_BULK_MODE_YN conf 값 설정
		XLLogger.outputInfoLog("[CONF] XL_BULK_MODE_YN  = " + XL_BULK_MODE_YN);
		
		// cksohn - XL_INIT_SCN
		if ( XL_INIT_SCN != 0 ) {
			XLLogger.outputInfoLog("##########################################################");
			XLLogger.outputInfoLog("[CONF] ###### XL_INIT_SCN  = " + XL_INIT_SCN + " #####");
			XLLogger.outputInfoLog("##########################################################");
		} else {
			XLLogger.outputInfoLog("[CONF] XL_INIT_SCN  = " + XL_INIT_SCN);
		}
		
		// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE start - [
		XLLogger.outputInfoLog("[CONF] XL_SRC_CHAR_RAWTOHEX_YN  = " + XL_SRC_CHAR_RAWTOHEX_YN);
		if ( XL_SRC_CHAR_RAWTOHEX_YN ) {
			XLLogger.outputInfoLog("[CONF] XL_SRC_CHAR_ENCODE  = " + XL_SRC_CHAR_ENCODE);	
		}
		
		// ] - end cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE
		
		XLLogger.outputInfoLog("[CONF] XL_DEBUG_YN  = " + XL_DEBUG_YN);
	}

	synchronized public static String getConfValue()
	{
	BufferedReader br = null; 
	try
	{
		br = new BufferedReader(new FileReader(XL_CONFIG));

		String line = "";
		StringBuffer sb = new StringBuffer();
		while( (line=br.readLine()) != null )
		{
			if( line.startsWith("#") ) continue;
			StringTokenizer st = new StringTokenizer(line, "=");
			if( st.countTokens()==0 ) continue;
			String varname = "";
			String value   = "";
			if( st.hasMoreTokens() ) varname = st.nextToken();
			if( st.hasMoreTokens() ) value   = st.nextToken();

			if( value.equals("") ) value = "*";
			sb.append( varname+"&"+value+"&" );
		}
		//br.close();
		return sb.toString();
		
	}catch( Exception e){ 		
		XLLogger.out.debug("getConfValue : " + e.toString());
		// NRAPException.outputExceptionLog(e);
		e.printStackTrace();
		return ""; 
	} finally {
		try {if (br != null) br.close();} catch (Exception e) {e.printStackTrace();}finally{br=null;}
	}
	}



	// cksohn - nr.conf advanced read ( "=" processing ) start - [ 
	synchronized public static String getConfValue( String _confname )
	{
		BufferedReader br = null;
		try
		{
			br = new BufferedReader(new FileReader(XL_CONFIG));
	
			String line = "";
			while( (line=br.readLine()) != null )
			{
				if( line.startsWith("#") ) continue;
				
				int idx = line.indexOf("=");
				
				String varname = "";
				String value = "";
				if ( idx >= 0 ) {
					varname = line.substring(0, idx);
					value = line.substring(idx+1, line.length());
					
				} else {
					// invalid cong value
					continue;
				}
					
				if( value.equals("") ) value = "*";				
				if( varname.equalsIgnoreCase(_confname) ) return value.trim(); 
			}
			//br.close();
			return "";
			
		}catch( Exception e){ 
			e.printStackTrace(); 
			return ""; 
	    } finally {
	        try {
	            if (br != null) br.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	    }
	}
	// ] - end cksohn - nr.conf advanced read ( "=" processing )
	
	/*
	*  특정 환경변수값을 가져온다.
	*/
	public static String getConfValue( String pFileName, String pEnv )
	{
		
        FileReader     fr = null;
        BufferedReader br = null;

		try
		{
			fr = new FileReader(pFileName);
			br = new BufferedReader(fr);
			String         strLine= "";
			String         envStr = "";
			String         valStr = "";
			Vector         envVec = null;
			boolean        addFlag = true;

			while( (strLine=br.readLine()) != null )
			{
				envStr = "";
				valStr = "";

				if( strLine.startsWith("#") ) continue;
				StringTokenizer st = new StringTokenizer(strLine, "=");
				if( st.hasMoreTokens() ) envStr = st.nextToken().trim();
				if( st.hasMoreTokens() ) valStr = st.nextToken().trim();
				if( envStr.equals(pEnv) ){
					//br.close();
					return valStr;
				}
			}
		}
		
		catch(Exception e){ 
			e.printStackTrace();
		} finally {
            try{if (br != null) br.close();}catch(Exception e){}finally{br = null;}
            try{if (fr != null) fr.close();}catch(Exception e){}finally{fr = null;}
		}
		
		return null;
	}

	
	public static void main(String[] args) 
	{
int a;
	}
}
