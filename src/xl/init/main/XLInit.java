package xl.init.main;

import java.io.File;
import java.sql.Connection;

import xl.init.logger.XLLogger;
import xl.lib.common.XLCommonVersion;
import xl.init.conf.XLConf;
import xl.init.conf.XLVersion;
import xl.init.dbmgr.XLMDBManager;
import xl.init.info.XLMemInfo;
import xl.init.poll.XLInitThread;
import xl.init.poll.XLPollingEventQ;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;


public class XLInit {

	
	// default environment
	public static String XL_HOME = "/opt/XL";
	public static String XL_DIR = "/opt/XL/XLInit";
	
	// 종료 요청시 FLAG
	public static boolean STOP_FLAG = false;
	
	
	// polling을 위한 event
	//public static XLPollingEventQ POLLING_EVENTQ = null;
	
	public static XLOptions epOptions = new XLOptions();
	
	// 초기 구동시 JOBQ에 남아 있는 R-->F, W->C
	private static boolean startCleanMode = false;
	
	public static String VERSION = "1.0 Sample";


	// 사용자 중지 또는 비정상 종료시 stopFlag false --> true
	private boolean stopJobFlag = false; 
	
	public static String polCode = "";
	
	public static String grpCode = "";
	
	public static String tableName = "";
		
	public static String bulkMode = "";
	
	public static boolean empty_chk = false;
	
	public static Integer commit_count;
	
	public static Integer parallel;
	
	public static Integer batch_size;
	
	public static Integer fetch_size;
	
	public XLInit() 
	{
	}
	
	public static void main(String [] args) 
	{
		try{
			
			try {
				if ( !checkOptions(args) ) {
					System.exit(0);
				} else {
					
					/*
					 *  [check input options : -g, -p, -t, -bulk_mode, -paral, -bs, -ps]
					 * 
					 *  EX)./xlinit -g GMP01 -p POP101 -t XLADMIN.XL_TEST01 -bulk_mode y -commit_ct 100000 -paral 2 -bs 2000 -ps 3000
					 */
					
					// ./xlinit -v
					if ( epOptions.isVersion() ) {
						System.out.println("X-LOG INIT Version V " + XLVersion.VERSION + "   build " + XLVersion.BUILD);
						System.exit(0);
					}
					
					// ./xlinit -help
					if ( epOptions.isHelp() ) {
						epOptions.usage();
						System.exit(0);
					}
					
					// 키워드 -g(대문자), -p(대문자), -t, -bulk_mode 필수 체크
					if(	epOptions.getGrp_code()!=null && isStringLowerCase(epOptions.getGrp_code()) &&
					    epOptions.getPol_code()!=null && isStringLowerCase(epOptions.getPol_code()) &&
					    epOptions.getTable()!=null &&
					    epOptions.getBulk_mode()!=null &&
					    // bulk_mode 옵션은 대소문자 구분하지 않음
					   (epOptions.getBulk_mode().equalsIgnoreCase("y") || epOptions.getBulk_mode().equalsIgnoreCase("n")))
					{
						 
						empty_chk = true;
						
						bulkMode = epOptions.getBulk_mode();
						grpCode = epOptions.getGrp_code();
						polCode = epOptions.getPol_code();
					    tableName = epOptions.getTable();
					    
					    // 키워드 -commit_ct, -pl, -bs, -fs => 필수  x. 키워드 안넣어도 기본값 set
					    if( epOptions.getCommit_count()!=null){
					    	commit_count = epOptions.getCommit_count();
					    }
					    if( epOptions.getParallel()!=null){
					    	parallel = epOptions.getParallel();
					    }
					    if( epOptions.getBatch_size()!=null){
					    	batch_size = epOptions.getBatch_size();
					    }
					    if( epOptions.getFetch_size()!=null){
					    	fetch_size = epOptions.getFetch_size();
					    }
					}		
				}
				
				if(!empty_chk)
				{
					System.out.println("[EXCEPTION] Invalid options."); 
					epOptions.usage();
					System.exit(0);
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
            
            XLInit server = new XLInit();
            if ( !server.init() )  {
    			XLLogger.outputInfoLog("Failed to start X-LOG Init.");
    			System.exit(0);
            }
            
            server.run();
             			
		}
		catch(Exception e){
			e.printStackTrace();
			
			System.out.println("[EXCEPTION] X-LOG INIT shutdown because occur error.");

			System.exit(0);
		}
	}
	
	private static boolean checkOptions(String[] _args)
	{

        CmdLineParser parser = new CmdLineParser(epOptions);
        try {
                parser.parseArgument(_args);
                
                return true;
        } catch (CmdLineException e) {
               	
            // handling of wrong arguments
        	System.out.println("[EXCEPTION] Invalid options.");
        	XLOptions.usage();
        	
            return false;
        }
	}
	
	// ayzn - str 소문자 체크
	private static boolean isStringLowerCase(String str)
	{		
		char[] charArray = str.toCharArray();

		for(int index = 0; index < charArray.length; index++){
			
		    if( Character.isLowerCase( charArray[index] )) {
		    	return false;
			}
		}
		
		return true;
	}
	
	public boolean init()
	{
		try {
			//---------------------------------------------------
			// sets a XL_HOME environement
			//---------------------------------------------------			
			XL_HOME = System.getenv("XL_HOME");	
			
			if ( XL_HOME == null || XL_HOME.equals("") ) {
				System.out.println("[EXCEPTION] XL_HOME environment value not defined.");
				// statusflag = false;
				// return statusflag;
				return false;
			}

			//---------------------------------------------------
			// sets a XL_DIR environement
			//---------------------------------------------------			
			XL_DIR = System.getenv("XL_DIR");	
			
			
			if( XL_DIR == null || XL_DIR.equals("") )
			{
				XL_DIR = XL_HOME + File.separator + "XLManager"; // cksohn - for mssql - XL_HOME & NR_HOME 수용
				return false;
			}
						
			//---------------------------------------------------
			// read conf file & catalog conf
			//---------------------------------------------------
			XLConf.initConf();	

			return true;

		} catch( Exception e )
		{
			XLLogger.outputInfoLog(e.getMessage());
			return false;
		}
	}

	public static void shutdown()
	{
		XLLogger.out.debug("X-LOG INIT shutdown.");
		System.exit(0);
	}
	
	public void setStopJobFlag(boolean stopJobFlag) {
		this.stopJobFlag = stopJobFlag;
	}

	public boolean isStopJobFlag() {
		return stopJobFlag;
	}

	public void run()
	{		
		XLLogger.outputInfoLog("");
		XLLogger.outputInfoLog("====================================================================================");
		XLLogger.outputInfoLog("X-LOG Init Version :::  " + XLVersion.VERSION + "    build " + XLVersion.BUILD);
		// cksohn - VERSION 정보 추가 로깅
		XLLogger.outputInfoLog("COMMON    Version :::  " + XLCommonVersion.VERSION);
		XLLogger.outputInfoLog("====================================================================================");
		
		
		// cksohn - scheduler 1.0 최초 버전 수정
		XLConf.writeLogConf();
		
			// 1. DBMS 정보 추출 및 메모리 등록
			XLMemInfo.registDbmsInfo();
			
				// grp_code, pol_code, table 전달
				XLInitThread pt = new XLInitThread(epOptions.getGrp_code(),epOptions.getPol_code(),epOptions.getTable());
				pt.run();
	}	
	
	
	public String getpolCode() {
		return polCode;
	}

	public void setgetpolCode(String polCode) {
		this.polCode = polCode;
	}
	
	public String getgrpCode() {
		return grpCode;
	}

	public void setgetgrpCode(String grpCode) {
		this.grpCode =grpCode;
	}
	
	public String gettableName() {
		return tableName;
	}

	public void setgettableName(String tableName) {
		this.tableName =tableName;
	}

	public String getbulkMode() {
		return bulkMode;
	}

	public void setgetbulkMode(String bulkMode) {
		this.bulkMode =bulkMode;
	}
}
