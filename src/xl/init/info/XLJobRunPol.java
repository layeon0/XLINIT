package xl.init.info;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;
import java.util.StringTokenizer;

import xl.lib.common.XLCons;
import xl.init.conf.XLConf;
import xl.init.conn.XLMariaDBConnection;
import xl.init.dbmgr.XLDBCons;
import xl.init.dbmgr.XLMDBManager;
import xl.init.engine.altibase.XLAltibaseApplyThread;
import xl.init.engine.altibase.XLAltibaseRecvBulkThread;
import xl.init.engine.altibase.XLAltibaseRecvThread;
import xl.init.engine.cubrid.XLCubridApplyThread;
import xl.init.engine.cubrid.XLCubridRecvThread;
import xl.init.engine.mariadb.XLMariaDBApplyBulkThread;
import xl.init.engine.mariadb.XLMariaDBApplyThread;
import xl.init.engine.mariadb.XLMariaDBRecvBulkThread;
import xl.init.engine.mariadb.XLMariaDBRecvThread;
import xl.init.engine.mssql.XLMSSQLApplyBulkThread;
import xl.init.engine.mssql.XLMSSQLApplyThread;
import xl.init.engine.mssql.XLMSSQLRecvBulkThread;
import xl.init.engine.mssql.XLMSSQLRecvThread;
import xl.init.engine.mysql.XLMySQLApplyBulkThread;
import xl.init.engine.mysql.XLMySQLApplyThread;
import xl.init.engine.mysql.XLMySQLRecvBulkThread;
import xl.init.engine.mysql.XLMySQLRecvThread;
import xl.init.engine.oracle.XLOracleApplyBulkThread;
import xl.init.engine.oracle.XLOracleApplyThread;
import xl.init.engine.oracle.XLOracleLinkThread;
import xl.init.engine.oracle.XLOracleRecvBulkThread;
import xl.init.engine.oracle.XLOracleRecvThread;
import xl.init.engine.postgresql.XLPostgreSQLApplyBulkThread;
import xl.init.engine.postgresql.XLPostgreSQLApplyThread;
import xl.init.engine.postgresql.XLPostgreSQLRecvBulkThread;
import xl.init.engine.postgresql.XLPostgreSQLRecvThread;
import xl.init.engine.ppas.XLPPASApplyBulkThread;
import xl.init.engine.ppas.XLPPASApplyThread;
import xl.init.engine.ppas.XLPPASRecvBulkThread;
import xl.init.engine.ppas.XLPPASRecvThread;
import xl.init.engine.tibero.XLTiberoApplyBulkThread;
import xl.init.engine.tibero.XLTiberoApplyThread;
import xl.init.engine.tibero.XLTiberoRecvBulkThread;
import xl.init.engine.tibero.XLTiberoRecvThread;
import xl.init.logger.XLLogger;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;
import xl.init.util.XLUtil;

/**
 * 
 * @author cksohn
 * 
 * 수행중인 정책  정보
 *
 */
public class XLJobRunPol {
	
	// 정책 기본 정보
	// ayzn - XLInit 기능 개발  - XLJobRunPol : jobseq 주석
	//private long 	jobseq = 0;
	private String 	polName = "";
	
	// ayzn - XLInit 기능 개발  - XLJobRunPol : 옵션 grpcode, tableName 추가
	private String 	grpCode = "";
	private String 	tableName = "";
	private String 	condWhere = ""; // where 조건
	
	// ayzn - XLInit 기능 개발  - XLJobRunPol : source 테이블이름
	private String 	tbSName = "";
	
	private int 	polCommitCnt = 100000; // commit 단위
	private String 	cpu_chkpoint = "S"; // S: source only, T:Target only, A: Source & Target 	
	private int 	tmaxJobCnt = 1; // 테이블별(즉 정책별) 동시 최대 작업수	
	private int		priority = 1;
	
	private long 	condCommitCnt = 0; // 지금까지 수행되어 누적된 commit 건수
	private long 	condSeq = 0; // XL_CONDITION 테이블의 SEQ
	
	
	private String	schedName = "";
	
	// src db info
	private XLDBMSInfo sdbInfo = null;
	// tar db info
	private XLDBMSInfo tdbInfo = null;
	
	// src & tar 테이블 정보
	private XLJobTableInfo tableInfo = null;
	
	// sql 구문 생성 정보
	private String 	idxHint = ""; // 소스 기준 - delete시 사용
	private int 	parallel= 1; // parallel hint 수
	private boolean orderByYN = false; 
	
	private String 	srcSelectSql = "";
	private String 	tarInsertSql = "";
	
	
	// JOB 수행 시작 시간 yyyy-mm-dd HH24:MM:SS
	private String sDate = "";
	
	
	// cksohn - BULK mode oracle sqlldr start - [
	// private int		exeMode 	= XLCons.BULK_MODE; // 추후 정책설정 mode에 따라 변경 필요
	// cksohn - XL_CAP_READ_REDO_YN - switching 시 데이터 누락 보완 - sender check
	private int		exeMode 	= XLOGCons.NORMAL_MODE; // 추후 정책설정 mode에 따라 변경 필요
	
	private String	bulk_pipePath = "";
	private String	bulk_ctlFilePath = "";
	
	
	// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
	private String	bulk_logFilePath = "";
	
	// ] - end
	
	// gssg - LG엔솔 MS2O
	// gssg - gssg - 공장코드 값 처리
	String customColname = "";
	String customValue = "";
	
	
	// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
	// sqlldr의 수행여부/상태 지정
	private boolean	runLoader = false;
	
	// gssg - xl 전체적으로 보완
    // gssg - m2m bulk mode thread 순서 조정
	private boolean loadQuery = false;
	
	// gssg - xl t2t 지원
	// gssg - t2t bulk mode 스레드 순서 조정
	// recvBulk 쓰레드의 수행여부
	private boolean writeYN = false;
	
	// gssg - 일본 네트워크 분산 처리
	private long workPlanSeq = 0;
	
	//#######################################################
	// 수행중인 정책(JOB)의 Recv Thread와 Apply Thread간 데이터 내부 Q
	//#######################################################
	private XLDataQ dataQ = new XLDataQ(); 
	
	//#################################################
	// DBMS 타입별로 추후 모두 생성해 주도록. or 추후 다른 방안 필요
	//#################################################
	// ORACLE
	private XLOracleRecvThread oraRecvThread = null;
	private XLOracleApplyThread oraApplyThread = null;
		
	// cksohn - BULK mode oracle sqlldr
	private XLOracleRecvBulkThread oraRecvBulkThread = null;
	private XLOracleApplyBulkThread oraApplyBulkThread = null;
	
	// gssg - SK실트론 O2O
	// gssg - linkMode 지원
	private XLOracleLinkThread oraLinkThread = null;
	
	// gssg - 한국전파 소스 오라클로 변경
	// gssg - oracle fileThread 개발
	// ayzn - XLInit 기능 개발  - XLJobRunPol : init에서 사용하지않는 file생성기능 주석
	//private XLOracleRecvBulkFileThread oraRecvBulkFileThread = null;
	
	// gssg - xl m2m 최초 포팅 개발 - TODO
	// MySQL
	// gssg - xl m2m 기능 추가
	private XLMySQLRecvThread mySQLRecvThread = null;
	private XLMySQLApplyThread mySQLApplyThread = null;
	
	// gssg - xl o2m 지원
	private XLMySQLRecvBulkThread mySQLRecvBulkThread = null;
	private XLMySQLApplyBulkThread mySQLApplyBulkThread = null;
	
	// gssg - xl m2m 기능 추가  - 0413
	// MariaDB
	private XLMariaDBRecvThread mariaDBRecvThread = null;
	private XLMariaDBApplyThread mariaDBApplyThread = null;
	
	// gssg - xl m2m bulk mode 지원
	private XLMariaDBRecvBulkThread mariaDBRecvBulkThread = null;
	private XLMariaDBApplyBulkThread mariaDBApplyBulkThread = null;
			
	// cksohn - xl o2p 기능 추가 start - [
	// PPAS
	// private XLIMPPASRecvThread ppasRecvThread = null; // 아직 미개발
	// gssg - xl o2p bulk mode 지원
	// gssg - ppas apply bulk thread 추가
	// gssg - xl p2p 지원
	private XLPPASRecvThread ppasRecvThread = null;
	private XLPPASApplyThread ppasApplyThread = null;
	
	private XLPPASRecvBulkThread ppasRecvBulkThread = null;
	private XLPPASApplyBulkThread ppasApplyBulkThread = null;
	
	// - end 
	
	// gssg - xl PPAS/PostgreSQL 분리
	private XLPostgreSQLRecvThread postgreSQLRecvThread = null;
	private XLPostgreSQLApplyThread postgreSQLApplyThread = null;
	
	private XLPostgreSQLRecvBulkThread postgreSQLRecvBulkThread = null;
	private XLPostgreSQLApplyBulkThread postgreSQLApplyBulkThread = null;
		
	// cksohn - xl tibero src 기능 추가 start - [
	private XLTiberoRecvThread tiberoRecvThread = null;
	private XLTiberoRecvBulkThread tiberoRecvBulkThread = null;
	// ] - end cksohn - xl tibero src 기능 추가
	// gssg - xl t2t 지원
	private XLTiberoApplyThread tiberoApplyThread = null;
	private XLTiberoApplyBulkThread tiberoApplyBulkThread = null;
	
	// gssg - csv file create 기능 추가
	// gssg - file create 클래스 분리					
	
		
	// gssg - ms2ms 지원
	private XLMSSQLRecvThread mssqlRecvThread = null;
	private XLMSSQLApplyThread mssqlApplyThread = null;
	
	private XLMSSQLRecvBulkThread mssqlRecvBulkThread = null;
	private XLMSSQLApplyBulkThread mssqlApplyBulkThread = null;
	
	// gssg - cubrid support
	private XLCubridRecvThread cubridRecvThread = null;
	private XLCubridApplyThread cubridApplyThread = null;
	
	// gssg - 국가정보자원관리원 데이터이관 사업
	// gssg - Altibase to Altibase 지원
	private XLAltibaseRecvThread altibaseRecvThread = null;
	private XLAltibaseApplyThread altibaseApplyThread = null;
	// gssg - Altibase to Oracle 지원
	private XLAltibaseRecvBulkThread altibaseRecvBulkThread = null;
	
	
	// job 상태
	private String jobStatus = XLOGCons.STATUS_RUNNING;
	
	// 쓰레드별 상태 
	private String errMsg_Recv = null;
	private String errMsg_Apply = null;
	
	// cksohn - BULK mode oracle sqlldr
	private String errMsg_Loader = null;
	
	// gssg - xl m2m bulk mode logging 지원
	private long applyCnt = 0; // gssg - xl recvCnt를 applyCnt로 적용
	

	// 사용자 중지 또는 비정상 종료시 stopFlag false --> true
	private boolean stopJobFlag = false; 
		
	private String logHead = "";
	
	
	public XLJobRunPol( String _grpCode, String _polName, String _tableName, String _condWhere) {
		this.grpCode = _grpCode;
		this.polName = _polName;
		this.tableName = _tableName;
		this.condWhere = _condWhere;
		
		this.tbSName = "";

		this.logHead = "[" + this.polName + "]";
	}
	
	// 정책정보 추출
	// public boolean makeInfo()
	public boolean makeInfo(Connection _cataConn)
	{
		try {
			
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			// ayzn - XLInit 기능 개발  - XLJobRunPol : commit_count, parallel 옵션 처리 추가
			if(XLConf.XL_COMMIT_COUNT>0)
			{
				this.polCommitCnt = XLConf.XL_COMMIT_COUNT;
			}	
			
			if(XLConf.XL_PARALLEL>1)
			{
				this.parallel = XLConf.XL_PARALLEL;
			}	
			
			// ayzn - XLInit 기능 개발  - XLJobRunPol :  수행 대상 정보 세팅 (Source, Target)
			Vector pol_info = null;
			pol_info = mDBMgr.getPolInfo(this.grpCode, this.polName);
			
			Hashtable<String, Hashtable<String, Object>> arr_pol = new Hashtable<>();

	        for ( int i=0; i<pol_info.size(); i++ ) 
			{
				Hashtable ht2 = (Hashtable)pol_info.get(i);
				
				String db_type = (String)ht2.get("GRP_TYPE");
				String db_ip = (String)ht2.get("SVR_IPADDR");
				String db_sid = (String)ht2.get("DBMS_SID");
				
				Hashtable<String, Object> element = new Hashtable<>();
		        element.put("SVR_IPADDR", db_ip);
		        element.put("DBMS_SID", db_sid);

		        arr_pol.put(db_type, element);
			}
			
	        
			String dicOwner = "";
			String dicTname = "";
			
			StringTokenizer tokenizer = new StringTokenizer(this.tableName, ".");
			while(tokenizer.hasMoreTokens()){            
	            if( tokenizer.hasMoreTokens() ) dicOwner = tokenizer.nextToken().trim();
				if( tokenizer.hasMoreTokens() ) dicTname = tokenizer.nextToken().trim();
			} 
			
			XLLogger.outputInfoLog("dicOwner = "+dicOwner);
			XLLogger.outputInfoLog("dicTname = "+dicTname);
			
			this.tbSName = dicTname ;
			
			// 1. 수행해야 할 JOB에 대한 정보를 DB로 부터 추출하여 setting 한다.
			// ayzn - XLInit 기능 개발  - XLJobRunPol : getJobRunPolInfo 함수처리 시 조건절 추가 및 변경
			//Vector vt = mDBMgr.getJobRunPolInfo(_cataConn, this.jobseq);
			Vector vt = mDBMgr.getJobRunPolInfo(_cataConn, this.grpCode, this.polName, dicOwner, dicTname);
			
			XLLogger.outputInfoLog("getJobRunPolInfo"); 
			XLLogger.outputInfoLog(vt);
			
			// DBMS 정보 추출
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Job - " + this.polName + "/" + condWhere);
				return false;
			}
			
			if ( vt.size() == 0 ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Job [NO Information] - " + this.polName + "/" + condWhere);
				return false;
				
			}
			XLLogger.outputInfoLog("##################  getJobRunPolInfo select  ####################");			
			
			// 테이블 및 컬럼 정보			
			for ( int i=0; i<vt.size(); i++ ) {
				
				Hashtable ht = (Hashtable)vt.get(i);
				
				if ( i == 0 ) { // pol 정보는 한번만 해주면 된다.

					
					// pol 정보
					// ayzn - XLInit 기능 개발  - XLJobRunPol : CDC 카탈로그 테이블에 없는 설정값 주석처리
					/*int polCommit = Integer.parseInt((String)ht.get("POL_COMMIT"));
					String polCpuChkpoint = (String)ht.get("POL_CPU_CHKPOINT");
					int polTMaxCnt = Integer.parseInt((String)ht.get("POL_TMAX_JOBCNT"));
					int polPriority = Integer.parseInt((String)ht.get("POL_PRIORITY"));
					
					String polIdxHint = (String)ht.get("POL_IDXHINT");
					int polParallel = Integer.parseInt((String)ht.get("POL_PARALLEL"));
					String polOrderByYNStr = (String)ht.get("POL_ORDERBY_YN");
					String polSchedName = (String)ht.get("SCHED_NAME");
					
					this.condCommitCnt = Long.parseLong((String)ht.get("CONDITION_COMMIT_CNT"));					
					this.condSeq = Long.parseLong((String)ht.get("CONDITION_SEQ"));
					
					this.condWhere = (String)ht.get("CONDITION_WHERE"); // 여기서 한번 더 셋팅
					
					// gssg - 일본 네트워크 분산 처리
					this.workPlanSeq = Integer.parseInt((String)ht.get("WORKPLAN_SEQ"));
					
					// XLLogger.outputInfoLog("CKSOHN DEBUG------ this.condCommitCnt-->>>>>>>>>>>>> " + this.condCommitCnt);
					
					this.polCommitCnt = polCommit;
					this.cpu_chkpoint = polCpuChkpoint;
					this.tmaxJobCnt = polTMaxCnt;
					this.priority = polPriority;
					this.idxHint = polIdxHint;
					this.parallel = polParallel;
					if ( polOrderByYNStr.equals("Y") ) {
						this.orderByYN = true;
					} else {
						this.orderByYN = false;
					}
					
					this.schedName = polSchedName;*/
					
					// src & tar db 정보
				    String sdbIp = (String)arr_pol.get("S").get("SVR_IPADDR"); 
				    String sdbSid =(String)arr_pol.get("S").get("DBMS_SID"); 
				    String tdbIp =(String)arr_pol.get("T").get("SVR_IPADDR"); 
				    String tdbSid =(String)arr_pol.get("T").get("DBMS_SID");
					
					String sOwner = (String)ht.get("DIC_OWNER");
					String sTable = (String)ht.get("DIC_TNAME");
					String tOwner = (String)ht.get("TB_TOWNER");
					String tTable = (String)ht.get("TB_TNAME");
										
					this.sdbInfo = XLMemInfo.HT_DBMS_INFO.get(sdbIp + "_" + sdbSid);
					this.tdbInfo = XLMemInfo.HT_DBMS_INFO.get(tdbIp + "_" + tdbSid);
					
					XLLogger.outputInfoLog("Source IP    = "+sdbIp);
				    XLLogger.outputInfoLog("Source SID   = "+sdbSid);
				    XLLogger.outputInfoLog("Source Owner = "+sOwner); 
					XLLogger.outputInfoLog("Source Table = "+sTable); 
					XLLogger.outputInfoLog("");
				    XLLogger.outputInfoLog("Target IP    = "+tdbIp);
				    XLLogger.outputInfoLog("Target SID   = "+tdbSid);
					XLLogger.outputInfoLog("Target Owner = "+tOwner); 
					XLLogger.outputInfoLog("Target Table = "+tTable); 
					
					// gssg - SK실트론 O2O -- start
					// gssg -  FROM 절에 사용자가 정의한 값 그대로 들어가는 기능 지원
					// ayzn - XLInit 기능 개발  - XLJobRunPol : CDC 카탈로그 테이블에 없는 설정값 주석처리
					//	String selectScript = (String)ht.get("POL_SELECT_SCRIPT");
					String selectScript = "";
					
					// gssg - linkMode 지원
					// ayzn - XLInit 기능 개발  - XLJobRunPol : CDC 카탈로그 테이블에 없는 설정값 주석처리
					//String dblinkName = (String)ht.get("POL_DBLINK_NAME");
					String dblinkName = "";
					
					

					this.tableInfo = new XLJobTableInfo(sOwner, sTable, tOwner, tTable, selectScript, dblinkName);
					
					// gssg - SK실트론 O2O -- end
					
					
				
				}
				
				
				// 3. Table 컬럼별 정보 생성
				// ayzn - XLInit 기능 개발  - XLJobRunPol : Table 컬럼 정보 생성 시 CDC 카탈로그 테이블 참조로 변경
				String colName =  (String)ht.get("DIC_COLNAME");
				String colNameMap = (String)ht.get("DIC_COLNAME_MAP");
				String dataTypeStr = (String)ht.get("DIC_DATATYPE");				
				int dataType = XLDicInfo.convertDataType(dataTypeStr);
				
				int colId = Integer.parseInt((String)ht.get("DIC_COLID"));

				String logmnrYN = (String)ht.get("DIC_LOGMNR_YN");
						
				// gssg - xl function 기능 지원
				// gssg - dicinfo_function 컬럼 추가
				String functionStr = (String)ht.get("DIC_FUNCTION");

				/// gssg - 소스 Function 기능 추가
				String functionStrSrc = (String)ht.get("DIC_FUNCTION");
				
				
				// gssg - o2o damo 적용
				// gssg - dicinfo_sec_yn 컬럼 추가
				// ayzn - XLInit 기능 개발  - XLJobRunPol : CDC 카탈로그 테이블에 없는 설정값 주석처리
				//String secYN = (String)ht.get("DICINFO_SEC_YN");
				//String secMapYN = (String)ht.get("DICINFO_SEC_MAP_YN");
				String secYN = "";
				String secMapYN = "";
				// gssg - 소스 Function 기능 추가
				XLJobColInfo colInfo = new XLJobColInfo(colName, colNameMap, dataType, colId, logmnrYN, functionStr, functionStrSrc, secYN, secMapYN);
				this.tableInfo.addColInfo(colInfo);
				
			} // for-end
			
			
			
			
			// gssg - LG엔솔 MS2O
			// gssg - 공장코드 값 처리 -- start 
			/*
			 * Vector customVt = mDBMgr.getCustomCode(polName); if ( customVt.size() != 0 )
			 * { Hashtable customHt = (Hashtable)customVt.get(0); this.customColname =
			 * (String)customHt.get("DICINFO_COLNAME"); this.customValue =
			 * (String)customHt.get("DICCODE_VALUE"); }
			 */				
									
			// -- end
			
			// gssg - xl p2t 지원
			// gssg - p2t 하다가 lob check 보완
			if( sdbInfo.getDbType() == XLCons.ORACLE ) {
				// gssg - o2t 지원
				// gssg - interval type 예외처리
				if ( tdbInfo.getDbType() == XLCons.TIBERO ) {
					this.tableInfo.checkTiberoTypeYN();					
				} else if ( tdbInfo.getDbType() == XLCons.ORACLE ) {
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - Oracle to Oracle 타임존 처리
					this.tableInfo.checkOraToOraTypeYN();					
				} else {
					this.tableInfo.checkOraTypeYN();
				}
			} else if( sdbInfo.getDbType() == XLCons.MYSQL || sdbInfo.getDbType() == XLCons.MARIADB ) {
				this.tableInfo.checkMySQLTypeYN();
			} else if( sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.TIBERO ) {
				this.tableInfo.checkTiberoTypeYN();
			} else if( sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.ORACLE ) {
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Tibero to Oracle 타임존 처리
				this.tableInfo.checkOraToOraTypeYN();
			} 
			// gssg - xl 전체적으로 보완2
			// gssg - PostgreSQL 커넥터 처리
			else if( (sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.PPAS) || 
					(sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.POSTGRESQL) ) {
				// gssg - o2m 하다가 t2p bulk mode 보완
				this.tableInfo.checkTiberoTypeYN();
			}
			// gssg - 국가정보자원관리원 데이터이관 사업
			// gssg - t2m bulk mode 지원
			else if( (sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.MYSQL) || 
					(sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.MARIADB) ) {
				// gssg - o2m 하다가 t2p bulk mode 보완
				this.tableInfo.checkTiberoTypeYN();
			}
			// gssg - xl 전체적으로 보완2
			// gssg - PostgreSQL 커넥터 처리
			else if( sdbInfo.getDbType() == XLCons.PPAS || sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
				// gssg - o2m 하다가 p2p bulk mode 보완
				this.tableInfo.checkPPASTypeYN();
			}
			// gssg - 국가정보자원관리원 데이터이관 사업
			// gssg - Altibase to Oracle 지원
			// gssg - Altibase5 to Altibase7 지원
			else if( sdbInfo.getDbType() == XLCons.ALTIBASE || sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
				this.tableInfo.checkOraTypeYN();
			}
			// gssg - LG엔솔 MS2O
			// gssg - ms2ms normal mode 지원
			else if( sdbInfo.getDbType() == XLCons.MSSQL ) {
				this.tableInfo.checkMSSQLTypeYN();
			}

			// cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록 start - [
			// if ( XLConf.XL_BULK_MODE_YN ) {
			// gssg - SK실트론 O2O
			// gssg - linkMode 지원
			if ( this.tableInfo.getDblinkName() == null || this.tableInfo.getDblinkName().equals("") ) {
				
				if ( XLConf.XL_BULK_MODE_YN ) { 
					if ( this.tableInfo.isBigType_yn() ) {						
						this.exeMode = XLOGCons.NORMAL_MODE;
						XLLogger.outputInfoLog("BULK_MODE --> NORMAL_MODE " + this.tableInfo.getSowner() + "." + this.tableInfo.getStable() + " has big data type");						
					} else {		
						XLLogger.outputInfoLog("BULK_MODE");
						this.exeMode = XLOGCons.BULK_MODE;
					}
				} else {
						XLLogger.outputInfoLog("NORMAL_MODE");
						this.exeMode = XLOGCons.NORMAL_MODE;					
					}
				
			} else {
				
				this.exeMode = XLOGCons.LINK_MODE;

			}

			// ] - end cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록
			
			XLLogger.outputInfoLog(this.logHead + "make information is finished - " + this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
						
			// 4. 수행 구문 생성
			if ( !makeQuery() ) {
				// 구문 생성 실패
				return false;
			}
			
			
			return true;
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
			return false;
		}
	}
	
	private String array() {
		// TODO Auto-generated method stub
		return null;
	}

	// 소스 select & target insert PreparedStatement 구문 생성
	private boolean makeQuery()
	{
		
		//XLLogger.outputInfoLog("#### makeQuert Called!!!!!!!!!!!!!!!!!!!!!!!");
		
		try {
			
			XLLogger.outputInfoLog("# makeQuery start #");
			StringBuffer sb = new StringBuffer();
			
			
			// gssg - 국가정보자원관리원 데이터이관 사업
			// gssg - refactoring
//			StringBuffer lobSb = new StringBuffer();

			Vector<XLJobColInfo> vtColInfo = this.tableInfo.getVtColInfo();			
			
			// orderBy를 사용시
			Vector<String> vtOrderByCol = new Vector<String>();
			
			// 1. Src select 구문 생성
			// gssg - SK실트론 O2O -- start
			// gssg - linkMode 지원
			XLLogger.outputInfoLog("# parallel #" + this.parallel);
			XLLogger.outputInfoLog("# idxHint #" + this.idxHint);
			if ( this.tableInfo.getDblinkName() == null || this.tableInfo.getDblinkName().equals("") ) 
			{

				// gssg -  SELECT 절에 사용자가 정의한 값 그대로 들어가는 기능 지원
				if ( this.tableInfo.getSelectScript() == null || this.tableInfo.getSelectScript().equals("") ) {
				
					sb.append("SELECT ");
					if ( this.parallel > 1) {
						sb.append(" /*+ PARALLEL(").append(this.tableInfo.getStable()).append(" ").append(this.parallel).append(") */ ");
					}
					
					// TODO : indexHint 적용시 !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
					if ( this.idxHint != null && !this.idxHint.equals("") ) {
						sb.append(this.idxHint + " ");
					}
										
					
					// 1-1 select column
					
					for ( int i=0; i<vtColInfo.size(); i++) {
						// gssg -xl lob 타입 보완
						// gssg - LOB 컬럼 순서 끝으로 처리
//							if ( i !=0 ) {
//								sb.append(",");
//							}					

						XLJobColInfo colInfo = vtColInfo.get(i);
			
						// sb.append(colInfo.getColName());
						// cksohn - xlim LOB 타입 지원			
						// gssg - t2o LOB 컬럼 순서 끝으로 처리
						if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
						// cksohn - xl tibero src 기능 추가
						// if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO) {
							
							if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {						
								// sb.append("e." + colInfo.getColName() + ".getClobVal()");
								// cksohn - xl tibero src 기능 추가
								// sb.append("e." + colInfo.getColName() + ".getClobVal() AS " +  colInfo.getColName());
								
								// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
//								sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");
								
								// gssg -xl lob 타입 보완
								// gssg - LOB 컬럼 순서 끝으로 처리
								if ( i !=0 ) {
									// gssg - 국가정보자원관리원 데이터이관 사업
									// gssg - refactoring
//									lobSb.append(",");
									sb.append(",");
								}					
								if( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
																							
									// gssg - 소스 Function 기능 추가
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										// gssg - 국가정보자원관리원 데이터이관 사업
										// gssg - refactoring												
																		
//										lobSb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );
										sb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );

									} else {
										 // Tibero에서는 아래 getClobVal()에서 에러 발생함
										// gssg - 국가정보자원관리원 데이터이관 사업
										// gssg - refactoring
//										lobSb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															
										sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															

									}
									
								} else {
									// gssg - 소스 Function 기능 추가
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										// gssg - 국가정보자원관리원 데이터이관 사업
										// gssg - refactoring
//										lobSb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
										sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );

									} else {
										// gssg - 국가정보자원관리원 데이터이관 사업
										// gssg - refactoring
//										lobSb.append("\"" + colInfo.getColName() + "\"");								
										sb.append("\"" + colInfo.getColName() + "\"");								
									}
								}
										
					
							// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE
							} 
											
							else if ( colInfo.getDataType() == XLDicInfoCons.CHAR ||
											colInfo.getDataType() == XLDicInfoCons.VARCHAR2 ) {
								
								// gssg -xl lob 타입 보완
								// gssg - LOB 컬럼 순서 끝으로 처리
								if ( i !=0 ) {
									sb.append(",");
								}
								
								if ( XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
									// sb.append("RAWTOHEX(" + colInfo.getColName() + ")");
									// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
									sb.append("RAWTOHEX(\"" + colInfo.getColName() + "\")");
								} else {			
									
									// gssg - 소스 Function 기능 추가
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
									} else {
										sb.append("\"" + colInfo.getColName() + "\"");								
									}
									
									// sb.append(colInfo.getColName());
									// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
//									sb.append("\"" + colInfo.getColName() + "\"");
								}
				
							} else {
								
								if ( i !=0 ) {
									sb.append(",");
								}
														
								// gssg - 소스 Function 기능 추가
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}

								// sb.append(colInfo.getColName());
								// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
//								sb.append("\"" + colInfo.getColName() + "\"");
								
							}
							
						} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL ||  this.sdbInfo.getDbType() == XLCons.MARIADB ) {
							
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - refactoring
							if ( i != 0 ) {
								sb.append(",");
							}
							
							if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
								sb.append( colInfo.getFunctionStrSrc() + "(" + "`" + colInfo.getColName() + "`" + ")" );
							} else {
								sb.append("`" + colInfo.getColName() + "`");								
							}
							
						} else if ( this.sdbInfo.getDbType() == XLCons.PPAS || this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
							
							// gssg - xl p2p 지원
							// gssg - p2p LOB 컬럼 순서 끝으로 처리
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리
							if ( i != 0 ) {
								sb.append(",");
							}
							
							// gssg - 소스 Function 기능 추가
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - PostgreSQL to PostgreSQL 보완
							if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
//								sb.append( colInfo.getFunctionStrSrc() + "(" + colInfo.getColName() + ")" );
								sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
							} else {
//								sb.append( colInfo.getColName() );								
								sb.append("\"" + colInfo.getColName() + "\"");								
							}		
											
						} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
							
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - Altibase to Oracle 지원
							// gssg - Altibase5 to Altibase7 지원
								if ( i != 0 ) {
									sb.append(",");
								}
								
								// gssg - 소스 Function 기능 추가
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}
							
						} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) {
							
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - Altibase to Oracle 지원
							// gssg - Altibase5 to Altibase7 지원
								if ( i != 0 ) {
									sb.append(",");
								}
								
								// gssg - 소스 Function 기능 추가
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}
							
						} else {
							
							// gssg -xl lob 타입 보완
							// gssg - LOB 컬럼 순서 끝으로 처리
							if ( i !=0 ) {
								sb.append(",");
							}
							
							// gssg - 소스 Function 기능 추가
							if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
								sb.append( colInfo.getFunctionStrSrc() + "(" + colInfo.getColName() + ")" );
							} else {
								sb.append( colInfo.getColName() );								
							}

//							sb.append(colInfo.getColName());
						}
						
						if ( this.orderByYN ) {					
							if ( colInfo.getLogmnrYN().equals("Y") ) {
								vtOrderByCol.add(colInfo.getColName());
							}				
						}
						
					} // for-end
					
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - refactoring
//					sb.append(lobSb);
						
					sb.append(" FROM ");
					// sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
					// cksohn - xlim LOB 타입 지원
					// if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
					// cksohn - xl tibero src 기능 추가
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - tibero tsn 지정 기능 추가
					if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
						// sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable() + " e");
						
						// cksohn - XL_INIT_SCN start - [
						if ( XLConf.XL_INIT_SCN != 0 ) {
							
							// cksohn - XL_INIT_SCN - SCN 지정시 테이블 alias e 구문의 Syntax Error 문제
							// gssg - o2o damo 적용
							// gssg - as of scn 대소문자 구분 처리
//							sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
							sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"");

							sb.append(" AS OF SCN " + XLConf.XL_INIT_SCN);
							sb.append(" e");
							
						} else { // cksohn - XL_INIT_SCN - SCN 지정시 테이블 alias e 구문의 Syntax Error 문제
							
							// sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable() + " e");
							// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
							sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");

						}
					
					} else if (this.sdbInfo.getDbType() == XLCons.MYSQL || this.sdbInfo.getDbType() == XLCons.MARIADB) { // gssg - xl MySQL src 기능 추가
						// gssg - xl m2m 기능 추가
						sb.append("`" + this.tableInfo.getSowner() + "`" + "." + "`" + this.tableInfo.getStable() + "`" + " e");
						
					} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) { 				
						// gssg - 국가정보자원관리원 데이터이관 사업
						// gssg - Altibase to Oracle 지원
						// gssg - Altibase5 to Altibase7 지원
						sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");
						
					} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL || this.sdbInfo.getDbType() == XLCons.PPAS ) { 				
						// gssg - 국가정보자원관리원 데이터이관 사업
						// gssg - PostgreSQL to PostgreSQL 보완
						sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");
						
					} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) {
						// gssg - SK텔레콤 O2M, O2P
						sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");						
					} else {
						sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
					}
					
					// if ( this.condWhere != null && this.condWhere.length() > 0 ) {
					if ( this.condWhere != null && !this.condWhere.equals("NONE") ) {
						sb.append(" WHERE " + this.condWhere); 			
					}
					
				} else {
				
					sb.append("SELECT ");
					for ( int i=0; i<vtColInfo.size(); i++) {
						XLJobColInfo colInfo = vtColInfo.get(i);
							
						if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {						
							if ( i !=0 ) {
								sb.append(",");
							}					

						if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
								sb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );
							} else {
								sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															
							}								
						} else if ( colInfo.getDataType() == XLDicInfoCons.CHAR || colInfo.getDataType() == XLDicInfoCons.VARCHAR2 ) {
								if ( i !=0 ) {
									sb.append(",");
								}
								
								if ( XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
									sb.append("RAWTOHEX(\"" + colInfo.getColName() + "\")");
								} else {							
									
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
									} else {
										sb.append("\"" + colInfo.getColName() + "\"");								
									}
									
								}
								
							} else {
								
								if ( i !=0 ) {
									sb.append(",");
								}
														
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}

							}			
						if ( this.orderByYN ) {					
							if ( colInfo.getLogmnrYN().equals("Y") ) {
								vtOrderByCol.add(colInfo.getColName());
							}				
						}
					}
					sb.append(" FROM (");
					sb.append(this.tableInfo.getSelectScript());
					sb.append(" )" + " XLOG_" + this.tableInfo.getStable());
																
				}
				
					// orderBy 절 존재
					if (vtOrderByCol.size() > 0) {
						sb.append(" ORDER BY ");
						for (int i=0; i<vtOrderByCol.size(); i++) {
							if ( i != 0 ) {
								sb.append(",");
							}
							// sb.append(vtOrderByCol.get(i));
							// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
							if (this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							} 
							// gssg - xl 전체적으로 보완
							// gssg - m2m 대소문자 처리
							else if ( this.sdbInfo.getDbType() == XLCons.MYSQL || this.sdbInfo.getDbType() == XLCons.MARIADB ) {
								sb.append("`" + vtOrderByCol.get(i) + "`");						
							}
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - Altibase to Oracle 지원
							// gssg - Altibase5 to Altibase7 지원
							else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							}
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - PostgreSQL to PostgreSQL 보완						
							else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL || this.sdbInfo.getDbType() == XLCons.PPAS ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							}
							// gssg - SK텔레콤 O2M, O2P
							else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							}
							else {
								sb.append(vtOrderByCol.get(i));
							}
						}
							
					}			
					// gssg - SK실트론 O2O
					// gssg - alias 지원
					this.srcSelectSql = sb.toString();
					this.srcSelectSql = srcSelectSql.replace("XL_MGR_ALIAS", XLConf.XL_MGR_ALIAS);
					
					
					// 2. Tar insert PreparedStatement 구문 생성
					sb = new StringBuffer();
					
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - refactoring
//					lobSb = new StringBuffer();
					
					sb.append("INSERT INTO ");
					
					
					// sb.append(this.tableInfo.getTowner() + "." + this.tableInfo.getTtable());
					// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제			
					if (this.tdbInfo.getDbType() == XLCons.ORACLE || this.tdbInfo.getDbType() == XLCons.TIBERO ) {
						// gssg - SK텔레콤 O2M, O2P -- start
						// sb.append("\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"");
						if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
							sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );							
						} else {
							sb.append( this.tableInfo.getTowner() + "." + this.tableInfo.getTtable() );
						}
						// gssg - SK텔레콤 O2M, O2P -- end						

					} 
					// gssg - xl m2m 최초 포팅 개발
					// gssg - xl m2m 기능 추가
					else if(this.tdbInfo.getDbType() == XLCons.MYSQL || this.tdbInfo.getDbType() == XLCons.MARIADB ) {					
						
						 sb.append("`" + this.tableInfo.getTowner() + "`" + "." + "`" + this.tableInfo.getTtable() + "`");						
						
					}
					// gssg - xl 전체적으로 보완
					// gssg - o2p 대소문자 처리
					// gssg - xl 전체적으로 보완2
					// gssg - PostgreSQL 커넥터 처리
					else if( this.tdbInfo.getDbType() == XLCons.PPAS || this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) {
						// gssg - SK텔레콤 O2M, O2P -- start
						// sb.append("\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"");
						if ( this.sdbInfo.getDbType() == XLCons.PPAS || this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
							sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );							
						} else {
							sb.append( this.tableInfo.getTowner() + "." + this.tableInfo.getTtable() );
						}
						// gssg - SK텔레콤 O2M, O2P -- end
						
					}									
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - Altibase to Oracle 지원
					// gssg - Altibase5 to Altibase7 지원
					else if( this.tdbInfo.getDbType() == XLCons.ALTIBASE || this.tdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
						// gssg - SK텔레콤 O2M, O2P -- start						
						// sb.append("\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"");
						if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
							sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );							
						} else {
							sb.append( this.tableInfo.getTowner() + "." + this.tableInfo.getTtable() );							
						}
						// gssg - SK텔레콤 O2M, O2P -- end
					}
					// gssg - SK텔레콤 O2M, O2P -- start
					else if( this.tdbInfo.getDbType() == XLCons.MSSQL ) {
						
						sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );
												
					}
					// gssg - SK텔레콤 O2M, O2P -- end
					else {
						sb.append(this.tableInfo.getTowner() + "." + this.tableInfo.getTtable());
					}						
					// insert columns
					sb.append("(");
					
					// gssg - LG엔솔 MS2O
					// gssg - 공장코드 값 처리 -- start 
					if ( this.customColname != null && !this.customColname.equals("") ) {
						sb.append("\"" + this.customColname + "\",");				
					}
					// --end

					
					for (int i=0; i<vtColInfo.size(); i++) {

						// gssg -xl lob 타입 보완
						// gssg - LOB 컬럼 순서 끝으로 처리
//							if ( i != 0 ) {
//								sb.append(",");
//							}
						
						XLJobColInfo colInfo = vtColInfo.get(i);
						// sb.append(colInfo.getColName_map());
						// cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제
						
						// gssg -xl lob 타입 보완
						// gssg - o2o LOB 컬럼 순서 끝으로 처리
						if ( this.tdbInfo.getDbType() == XLCons.ORACLE || this.tdbInfo.getDbType() == XLCons.TIBERO ) {

//							
//							sb.append("\"" + colInfo.getColName_map() + "\"");

							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - refactoring
							if ( i != 0 ) {
								sb.append(",");
							}
							// gssg - SK텔레콤 O2M, O2P -- start
							// sb.append("\"" + colInfo.getColName_map() + "\"");
							if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
								sb.append( "\"" + colInfo.getColName_map() + "\"" );								
							} else {
								sb.append( colInfo.getColName_map() );
							}
							// gssg - SK텔레콤 O2M, O2P -- end										
						} else if ( this.tdbInfo.getDbType() == XLCons.PPAS || this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) {
		                     
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - PostgreSQL to PostgreSQL 보완						
							if ( i != 0 ) {
								sb.append(",");
							}
							// gssg - SK텔레콤 O2M, O2P -- start
							// sb.append("\"" + colInfo.getColName_map() + "\"");
							if ( this.sdbInfo.getDbType() == XLCons.PPAS || this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
								sb.append( "\"" + colInfo.getColName_map() + "\"" );								
							} else {
								sb.append( colInfo.getColName_map() );
							}
							// gssg - SK텔레콤 O2M, O2P -- end

		                  }  else if ( this.tdbInfo.getDbType() == XLCons.MYSQL || this.tdbInfo.getDbType() == XLCons.MARIADB ) {
							
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - refactoring
							if ( i != 0 ) {
								sb.append(",");
							}
								sb.append("`" + colInfo.getColName_map() + "`");																					
																													
						} else if ( this.tdbInfo.getDbType() == XLCons.ALTIBASE || this.tdbInfo.getDbType() == XLCons.ALTIBASE5 ) {

							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - Altibase to Oracle 지원				
							// gssg - Altibase5 to Altibase7 지원
							if ( i != 0 ) {
								sb.append(",");
							}
							// gssg - SK텔레콤 O2M, O2P -- start
							// sb.append("\"" + colInfo.getColName_map() + "\"");
							if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
								sb.append( "\"" + colInfo.getColName_map() + "\"" );								
							} else {
								sb.append( colInfo.getColName_map() );
							}
							// gssg - SK텔레콤 O2M, O2P -- end			
									
						} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) {
							
							if ( i != 0 ) {
								sb.append(",");
							}
							sb.append("\"" + colInfo.getColName_map() + "\"");							

						} else {
							// gssg -xl lob 타입 보완
							// gssg - LOB 컬럼 순서 끝으로 처리
							if ( i != 0 ) {
								sb.append(",");
							}				
							sb.append(colInfo.getColName_map());
						}

					}
					
					// gssg -xl lob 타입 보완
					// gssg - LOB 컬럼 순서 끝으로 처리
//					sb.append(lobSb);
					
					sb.append(")");
					
					// values ?
					sb.append(" VALUES (");
					
					
					// gssg - LG엔솔 MS2O
					// gssg - 공장코드 값 처리 -- start 
					if ( this.customValue != null && !this.customValue.equals("") ) {
						sb.append("'" + this.customValue + "',");				
					}
					// --end

					
					for (int i=0; i<vtColInfo.size(); i++) {
						if ( i != 0 ) {
							sb.append(",");
						}
						
						// sb.append("?");
						// cksohn - xlim LOB 타입 지원
						XLJobColInfo colInfo = vtColInfo.get(i);
						// if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
						if ( this.tdbInfo.getDbType() == XLCons.ORACLE ) {
							if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {
								// gssg - 대법원 O2O
								// gssg - raw_to_varchar2 기능 지원
								if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
									sb.append("sys.XMLType.createXML(" + colInfo.getFunctionStr() + ")");
								} else {
									sb.append("sys.XMLType.createXML(?)");	
								}									
								
							} else {						
								// gssg - xl function 기능 지원
								// gssg - insert values 부분에 함수 씌우기						
								if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
									// gssg - LG엔솔 MS2O
									// gssg - function 파라미터 여러개
									if ( colInfo.getFunctionStr().contains("?") ) {
										sb.append(colInfo.getFunctionStr());																
									} else {
										sb.append(colInfo.getFunctionStr() + "(?)");						
									}
									
								} else {
									sb.append("?");							
								}
							}
							
						} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) {
								// gssg - xl function 기능 지원
								// gssg - insert values 부분에 함수 씌우기						
								if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {									

									// gssg - 세븐일레븐 O2MS
									if ( colInfo.getFunctionStr().contains("?") ) {
										sb.append(colInfo.getFunctionStr());										
									} else {
										// 티베로에서는 "lower"(컬럼) 와 같이 내장함수에 쌍따옴표(")를 붙일 수 없음
										sb.append( colInfo.getFunctionStr() + "(?)");										
									}									
								} else {
									sb.append("?");							
								}					
						} else if ( this.tdbInfo.getDbType() == XLCons.PPAS || 
								this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // cksohn - xl o2p 기능 추가
						
								// gssg - xl 전체적으로 보완2
								// gssg - PostgreSQL 커넥터 처리
								if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE || colInfo.getDataType() == XLDicInfoCons.XML ) {						
									sb.append("XML(?)");
								} else if ( colInfo.getDataType() == XLDicInfoCons.INTERVAL_DS || colInfo.getDataType() == XLDicInfoCons.INTERVAL_YM ) {
									
									sb.append(" CAST(?  AS INTERVAL)");
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.BITVARYING ) {
																
									// gssg - xl p2p 지원
									// gssg - p2p normal mode 타입 처리
									sb.append("(?)::VARBIT");
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.BIT ) {
									
									// gssg - xl p2p 지원
									// gssg - p2p normal mode 타입 처리
									sb.append("(?)::VARBIT");
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.MONEY ) {
									
									// gssg - xl 전체적으로 보완2
									// gssg - PostgreSQL 커넥터 처리
									if ( this.sdbInfo.getDbType() == XLCons.PPAS || 
											this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
										sb.append(" CAST(?  AS MONEY)");
									} else {
										sb.append("?");
									}
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.TIME || colInfo.getDataType() == XLDicInfoCons.TIME_TZ ) {
									
									// gssg - xl 전체적으로 보완2
									// gssg - PostgreSQL 커넥터 처리
									if ( this.sdbInfo.getDbType() == XLCons.PPAS || 
											this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
										sb.append(" CAST(?  AS TIME)");
									} else {
										sb.append("?");
									}
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.NUMERIC || colInfo.getDataType() == XLDicInfoCons.DECIMAL ) {
									
									// gssg - 모듈 보완
									// gssg - P2P - normal NUMERIC 타입 보완
									if ( this.sdbInfo.getDbType() == XLCons.PPAS || 
											this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
										sb.append(" CAST(?  AS NUMERIC)");
									} else {
										sb.append("?");
									}
									
								}  else {							
									// gssg - xl function 기능 지원
									// gssg - insert values 부분에 함수 씌우기						
									if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {										
										// gssg - 세븐일레븐 O2MS
										if ( colInfo.getFunctionStr().contains("?") ) {
											sb.append(colInfo.getFunctionStr());										
										} else {
											// gssg - SK텔레콤 O2M, O2P
											if ( colInfo.getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
												sb.append("?");																								
											} else {
												sb.append( colInfo.getFunctionStr() + "(?)");												
											}											
										}									

									} else {
										sb.append("?");					
									}
								}
						} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL || this.tdbInfo.getDbType() == XLCons.MARIADB ) {

							// gssg - xl m2m 기능 추가 
							// gssg - xl function 기능 지원
							// gssg - insert values 부분에 함수 씌우기						
							if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
								// gssg - 세븐일레븐 O2MS
								if ( colInfo.getFunctionStr().contains("?") ) {
									sb.append(colInfo.getFunctionStr());										
								} else {
									sb.append( colInfo.getFunctionStr() + "(?)");										
								}									

							} else {
								sb.append("?");							
							}
						} else {					
							// gssg - ms2ms 지원
							// gssg - function 지원
							if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
								
								// gssg - 세븐일레븐 O2MS
								if ( colInfo.getFunctionStr().contains("?") ) {
									sb.append(colInfo.getFunctionStr());										
								} else {
									sb.append( colInfo.getFunctionStr() + "(?)");										
								}									
							} else {
								sb.append("?");							
							}
						
						}
						
					}
					
					sb.append(")");								
				
				this.tarInsertSql = sb.toString();
				
			
			} else {
				
				sb.append("INSERT INTO " + "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" + "@" + this.tableInfo.getDblinkName() + "(");
				
				for ( int i=0; i<vtColInfo.size(); i++) {
					XLJobColInfo colInfo = vtColInfo.get(i);
					
					if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {						
						if ( i !=0 ) {
							sb.append(",");
						}					

					if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
							sb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );
						} else {
							sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															
						}								
					} else if ( colInfo.getDataType() == XLDicInfoCons.CHAR || colInfo.getDataType() == XLDicInfoCons.VARCHAR2 ) {
							
							if ( i !=0 ) {
								sb.append(",");
							}
							
							if ( XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
								sb.append("RAWTOHEX(\"" + colInfo.getColName() + "\")");
							} else {							
								
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}
								
							}
							
						} else {
							
							if ( i !=0 ) {
								sb.append(",");
							}
													
							if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
								sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
							} else {
								sb.append("\"" + colInfo.getColName() + "\"");								
							}

						}

				}
				
			sb.append(") " );
			
			this.tarInsertSql = sb.toString();
			
			// gssg - SK실트론 O2O
			// gssg - alias 지원
			this.tarInsertSql = tarInsertSql.replace("XL_MGR_ALIAS", XLConf.XL_MGR_ALIAS);
			
			sb = new StringBuffer();
			
			sb.append("SELECT ");
			
			for ( int i=0; i<vtColInfo.size(); i++) {

				XLJobColInfo colInfo = vtColInfo.get(i);
				
				if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {						
					if ( i !=0 ) {
						sb.append(",");
					}					

				if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
						sb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );
					} else {
						sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															
					}								
				} else if ( colInfo.getDataType() == XLDicInfoCons.CHAR || colInfo.getDataType() == XLDicInfoCons.VARCHAR2 ) {
						
						if ( i !=0 ) {
							sb.append(",");
						}
						
						if ( XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
							sb.append("RAWTOHEX(\"" + colInfo.getColName() + "\")");
						} else {							
							
							if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
								sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
							} else {
								sb.append("\"" + colInfo.getColName() + "\"");								
							}
							
						}
						
					} else {
						
						if ( i !=0 ) {
							sb.append(",");
						}
												
						if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
							sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
						} else {
							sb.append("\"" + colInfo.getColName() + "\"");								
						}

					}			
				
			}
			
			sb.append(" FROM ");
			if ( this.tableInfo.getSelectScript() == null || this.tableInfo.getSelectScript().equals("") ) {				
				if ( XLConf.XL_INIT_SCN != 0 ) {
					
					sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"");
					sb.append(" AS OF SCN " + XLConf.XL_INIT_SCN);
					sb.append(" e");
					
				} else {					
					sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");
				}
				
			} else {
				sb.append("( ");
				sb.append(this.tableInfo.getSelectScript());
				sb.append(" ) XLOG_" + this.tableInfo.getStable());				
			}
			
			this.srcSelectSql = sb.toString();		

			// gssg - SK실트론 O2O
			// gssg - alias 지원
			this.srcSelectSql = srcSelectSql.replace("XL_MGR_ALIAS", XLConf.XL_MGR_ALIAS);

			}
			// gssg - SK실트론 O2O -- end

			
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- [JOB] " + this.polName + "/" + this.condWhere + " Query -----");
				
				// gssg - SK실트론 O2O
				// gssg - linkMode 지원
				if ( this.tableInfo.getDblinkName() == null || this.tableInfo.getDblinkName().equals("") ) {
					XLLogger.outputInfoLog("[DEBUG][QUERY] SRC SELECT : " + this.srcSelectSql);
					XLLogger.outputInfoLog("[DEBUG][QUERY] TAR INSERT : " + this.tarInsertSql);					
				} else {
					XLLogger.outputInfoLog("[DEBUG][QUERY] InsertSelect : " + this.tarInsertSql + this.srcSelectSql);
				}
				XLLogger.outputInfoLog("[DEBUG]-------------------------------------------------------------------");
			}
			

			return true;
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			return false;
			
		}
	}
	
	
	/// JOB 수행
	public void exeJob()
	{
		try {
			
			XLLogger.outputInfoLog(this.logHead + " START JOB : " + this.polName + "/" + this.condWhere);
			
			// 1. Recv Thread 생성
			byte sdbType = this.sdbInfo.getDbType();
						
			switch (sdbType) {
				case XLCons.ORACLE:
					oraRecvThread = new XLOracleRecvThread(this);
					oraRecvThread.start();
					break;
					
				// cksohn - xl tibero src 기능 추가
				case XLCons.TIBERO:
					tiberoRecvThread = new XLTiberoRecvThread(this);
					tiberoRecvThread.start();
					break;
					
				// gssg - xl m2m 최초 포팅 개발
				// gssg - xl m2m 기능 추가 
				case XLCons.MYSQL:
					mySQLRecvThread = new XLMySQLRecvThread(this);
					mySQLRecvThread.start();
					break;
				
				// gssg - xl m2m 기능 추가  - 0413
				case XLCons.MARIADB:
					mariaDBRecvThread = new XLMariaDBRecvThread(this);
					mariaDBRecvThread.start();
					break;
				
				// gssg - xl p2p 지원
				// gssg - xl 전체적으로 보완2
				case XLCons.PPAS:
					ppasRecvThread = new XLPPASRecvThread(this);
					ppasRecvThread.start();
					break;				
				
				// gssg - xl PPAS/PostgreSQL 분리
				case XLCons.POSTGRESQL:
					postgreSQLRecvThread = new XLPostgreSQLRecvThread(this);
					postgreSQLRecvThread.start();
					break;				
					
				// gssg - ms2ms 지원
				case XLCons.MSSQL:
					mssqlRecvThread = new XLMSSQLRecvThread(this);
					mssqlRecvThread.start();
					break;
				
				// gssg - cubrid support
				case XLCons.CUBRID:
					cubridRecvThread = new XLCubridRecvThread(this);
					cubridRecvThread.start();
					break;
					
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Altibase to Altibase 지원
				// gssg - Altibase5 to Altibase7 지원
				case XLCons.ALTIBASE:
				case XLCons.ALTIBASE5:				
					altibaseRecvThread = new XLAltibaseRecvThread(this);
					altibaseRecvThread.start();
					break;
					
					
			}
			
			Thread.sleep(500);
			
			// 2. Apply Thread 생성
			byte tdbType = this.tdbInfo.getDbType();
			switch (tdbType) {
				case XLCons.ORACLE:
					oraApplyThread = new XLOracleApplyThread(this);
					oraApplyThread.start();
					break;
				
				// gssg - xl t2t 지원
				case XLCons.TIBERO:
					tiberoApplyThread = new XLTiberoApplyThread(this);
					tiberoApplyThread.start();
					break;
					
				// cksohn - xl o2p 기능 추가
				// gssg - xl 전체적으로 보완2
				case XLCons.PPAS:
					ppasApplyThread = new XLPPASApplyThread(this);
					ppasApplyThread.start();
					break;
					
				// gssg - xl PPAS/PostgreSQL 분리
				case XLCons.POSTGRESQL:
					postgreSQLApplyThread = new XLPostgreSQLApplyThread(this);
					postgreSQLApplyThread.start();
					break;
					
				// gssg - xl m2m 최초 포팅 개발
				// gssg - xl m2m 기능 추가
				case XLCons.MYSQL:
					mySQLApplyThread = new XLMySQLApplyThread(this);
					mySQLApplyThread.start();
					break;
				
				// gssg - xl m2m 기능 추가  - 0413
				case XLCons.MARIADB:
					mariaDBApplyThread = new XLMariaDBApplyThread(this);
					mariaDBApplyThread.start();
					break;
					
				// gssg - ms2ms 지원
				case XLCons.MSSQL:
					mssqlApplyThread = new XLMSSQLApplyThread(this);
					mssqlApplyThread.start();
					break;
					
				// gssg - cubrid support
				case XLCons.CUBRID:
					cubridApplyThread = new XLCubridApplyThread(this);
					cubridApplyThread.start();
					break;
					
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Altibase to Altibase 지원
				// gssg - Altibase5 to Altibase7 지원
				case XLCons.ALTIBASE:
				case XLCons.ALTIBASE5:					
					altibaseApplyThread = new XLAltibaseApplyThread(this);
					altibaseApplyThread.start();
					break;
					
			}
			
			
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
		}
	}
	
	// cksohn - BULK mode oracle sqlldr
	public void exeJobBulk()
	{
		try {
			
			XLLogger.outputInfoLog(this.logHead + " START BULK JOB : " + this.polName + "/" + this.condWhere);
		
			
			// gssg - csv file create 기능 추가
			if ( XLConf.XL_CREATE_FILE_YN ) {
				XLLogger.outputInfoLog("---create file-----");
				// gssg - csv file create 기능 추가
				// gssg - csv 파일 생성
				this.bulk_pipePath = XLUtil.makeCSV(this);
			} else {				
				// pipe 파일 생성
				XLLogger.outputInfoLog("---makePipe-----");
				this.bulk_pipePath = XLUtil.makePipe(this.polName, this.tbSName);
			}
		
			
			// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL - 추가 수정 타겟이 오라클일때만
			// 추후 타겟 DB별로 별도 처리해야 함 !!!!!!!!!!!!!!!!!!!!!
			byte tdbType = this.tdbInfo.getDbType();
			if ( tdbType == XLCons.ORACLE ) {
				// ctlfile 생성
				this.bulk_ctlFilePath = XLUtil.makeCtlFile_ORACLE(this, this.tbSName);
			
			
				// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
				this.bulk_logFilePath = this.bulk_ctlFilePath.substring(0, this.bulk_ctlFilePath.lastIndexOf(".")) + ".log";
			
				
				XLLogger.outputInfoLog("bulk_logFilePath-> "+this.bulk_logFilePath);
				
			} else if ( tdbType == XLCons.TIBERO ) {
				// gssg - xl t2t 지원
				// gssg - t2t bulk mode 지원
				this.bulk_ctlFilePath = XLUtil.makeCtlFile_TIBERO(this, this.tbSName);
				
				this.bulk_logFilePath = this.bulk_ctlFilePath.substring(0, this.bulk_ctlFilePath.lastIndexOf(".")) + ".log";
			}
			
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			// 2. Apply Thread 생성
			// byte tdbType = this.tdbInfo.getDbType();
			switch (tdbType) {
				case XLCons.ORACLE:
					oraApplyBulkThread = new XLOracleApplyBulkThread(this);
					oraApplyBulkThread.start();
					break;
					
				// gssg - xl t2t 지원
				case XLCons.TIBERO:					
					tiberoApplyBulkThread = new XLTiberoApplyBulkThread(this);
					tiberoApplyBulkThread.start();
					break;
					
				case XLCons.MARIADB:
					mariaDBApplyBulkThread = new XLMariaDBApplyBulkThread(this);
					mariaDBApplyBulkThread.start();
					break;
					
				// gssg - xl o2m 지원
				case XLCons.MYSQL:
					mySQLApplyBulkThread = new XLMySQLApplyBulkThread(this);
					mySQLApplyBulkThread.start();
					break;
					
				// gssg - xl o2p bulk mode 지원
				// gssg - ppas apply bulk thread 추가
				case XLCons.PPAS:
					ppasApplyBulkThread = new XLPPASApplyBulkThread(this);
					ppasApplyBulkThread.start();
					break;
				
				// gssg - xl PPAS/PostgreSQL 분리
				case XLCons.POSTGRESQL:
					postgreSQLApplyBulkThread = new XLPostgreSQLApplyBulkThread(this);
					postgreSQLApplyBulkThread.start();
					break;
					
				// gssg - ms2ms 지원
				case XLCons.MSSQL:
					mssqlApplyBulkThread = new XLMSSQLApplyBulkThread(this);
					mssqlApplyBulkThread.start();
					break;
					
			}
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			int MAX_CHECK_CNT = 10;
			int chkCnt = 0;
			
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("DEBUG ----- START WHILE! - " + isRunLoader());
			}
						
			while ( !isRunLoader() && chkCnt <= MAX_CHECK_CNT ) {
				chkCnt++;
				XLLogger.outputInfoLog("[" + getPolName() + "][LOADER] Waiting Run Loader before RecvThread starting.(" + chkCnt + ")");
				Thread.sleep(1000);
			}
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("DEBUG ----- END WHILE! - " + isRunLoader());
			}
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정 - comment
			//XLLogger.outputInfoLog("CKSOHN DEBUG SLEEP TEST - !!!");
			// Thread.sleep(3000); //
			
			// 1. Recv Thread 생성
			byte sdbType = this.sdbInfo.getDbType();
			
			switch (sdbType) {
			
				case XLCons.ORACLE:
					
					// gssg - 한국전파 소스 오라클로 변경
				
					oraRecvBulkThread = new XLOracleRecvBulkThread(this);
					oraRecvBulkThread.start();
										
					
					break;
					
				// cksohn - xl tibero src 기능 추가
				case XLCons.TIBERO:
					
					// gssg - csv file create 기능 추가
					// gssg - file create 클래스 분리					
				
					tiberoRecvBulkThread = new XLTiberoRecvBulkThread(this);
					tiberoRecvBulkThread.start();						
					
					
					break;
					
				// gssg - xl m2m bulk mode 지원
				case XLCons.MARIADB:
					mariaDBRecvBulkThread = new XLMariaDBRecvBulkThread(this);
					mariaDBRecvBulkThread.start();
					break;
					
				// gssg - xl o2m 지원
				case XLCons.MYSQL:
					mySQLRecvBulkThread = new XLMySQLRecvBulkThread(this);
					mySQLRecvBulkThread.start();
					break;
				
				// gssg - xl p2p 지원
				case XLCons.PPAS:
					ppasRecvBulkThread = new XLPPASRecvBulkThread(this);
					ppasRecvBulkThread.start();
					break;
				
				// gssg - xl PPAS/PostgreSQL 분리
				case XLCons.POSTGRESQL:
					postgreSQLRecvBulkThread = new XLPostgreSQLRecvBulkThread(this);
					postgreSQLRecvBulkThread.start();
					break;
					
				case XLCons.MSSQL:
					mssqlRecvBulkThread = new XLMSSQLRecvBulkThread(this);
					mssqlRecvBulkThread.start();
					// gssg - LG엔솔 MS2O
					// gssg - ms2o bulk mode 지원
					break;
					
					
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Altibase to Oracle 지원
				// gssg - Altibase5 to Altibase7 지원
				case XLCons.ALTIBASE:
				case XLCons.ALTIBASE5:
					altibaseRecvBulkThread = new XLAltibaseRecvBulkThread(this);
					altibaseRecvBulkThread.start();
			}

			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정 - comment
			/***
			// 2. Apply Thread 생성
			byte tdbType = this.tdbInfo.getDbType();
			switch (tdbType) {
				case XLCons.ORACLE:
					oraApplyBulkThread = new XLOracleApplyBulkThread(this);
					oraApplyBulkThread.start();
					break;
			}
			***/
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
		}
	}
	
	// gssg - SK실트론 O2O
	// gssg - linkMode 지원
	public void exeJobLink() {

		try {
			
			XLLogger.outputInfoLog(this.logHead + " START JOB : " + this.polName + "/" + this.condWhere);
			
			// 1. Recv Thread 생성
			byte sdbType = this.sdbInfo.getDbType();
						
			switch (sdbType) {
				case XLCons.ORACLE:
					oraLinkThread = new XLOracleLinkThread(this);
					oraLinkThread.start();
					break;
			}
									
			} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
		}
	
		
	}
	
	// ayzn - XLInit 기능 개발  - XLJobRunPol : jobseq 주석
	/*public long getJobseq() {
		return jobseq;
	}
	public void setJobseq(long jobseq) {
		this.jobseq = jobseq;
	}*/
	public String getPolName() {
		return polName;
	}
	public void setPolName(String polName) {
		this.polName = polName;
	}
	public String getCondWhere() {
		return condWhere;
	}
	public void setCondWhere(String condWhere) {
		this.condWhere = condWhere;
	}
	public int getPolCommitCnt() {
		return polCommitCnt;
	}
	public void setPolCommitCnt(int polCommitCnt) {
		this.polCommitCnt = polCommitCnt;
	}
	public String getCpu_chkpoint() {
		return cpu_chkpoint;
	}
	public void setCpu_chkpoint(String cpu_chkpoint) {
		this.cpu_chkpoint = cpu_chkpoint;
	}
	public int getTmaxJobCnt() {
		return tmaxJobCnt;
	}
	public void setTmax_jobCnt(int tmaxJobCnt) {
		this.tmaxJobCnt = tmaxJobCnt;
	}
	
	
	
	public int getPriority() {
		return priority;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	public XLJobTableInfo getTableInfo() {
		return tableInfo;
	}
	public void setTableInfo(XLJobTableInfo tableInfo) {
		this.tableInfo = tableInfo;
	}
	public String getIdxHint() {
		return idxHint;
	}
	public void setIdxHint(String idxHint) {
		this.idxHint = idxHint;
	}
	public int getParallel() {
		return parallel;
	}
	public void setParallel(int parallel) {
		this.parallel = parallel;
	}
	public boolean isOrderByYN() {
		return orderByYN;
	}
	public void setOrderByYN(boolean orderByYN) {
		this.orderByYN = orderByYN;
	}
	public String getSrcSelectSql() {
		return srcSelectSql;
	}
	public void setSrcSelectSql(String srcSelectSql) {
		this.srcSelectSql = srcSelectSql;
	}
	public String getTarInsertSql() {
		return tarInsertSql;
	}
	public void setTarInsertSql(String tarInsertSql) {
		this.tarInsertSql = tarInsertSql;
	}
	
	public XLDBMSInfo getSdbInfo() {
		return sdbInfo;
	}

	public void setSdbInfo(XLDBMSInfo sdbInfo) {
		this.sdbInfo = sdbInfo;
	}

	public XLDBMSInfo getTdbInfo() {
		return tdbInfo;
	}

	public void setTdbInfo(XLDBMSInfo tdbInfo) {
		this.tdbInfo = tdbInfo;
	}

	


	public boolean isStopJobFlag() {
		return stopJobFlag;
	}

	public void setStopJobFlag(boolean stopJobFlag) {
		this.stopJobFlag = stopJobFlag;
	}

	public XLDataQ getDataQ()
	{
		return this.dataQ;
	}
	
	public String getErrMsg_Recv() {
		return errMsg_Recv;
	}

	public void setErrMsg_Recv(String errMsg_Recv) {
		this.errMsg_Recv = errMsg_Recv;
		
		// this.jobStatus = XLMgrCons.STATUS_FAIL;
	}

	public String getErrMsg_Apply() {
		return errMsg_Apply;
		
	}

	public void setErrMsg_Apply(String errMsg_Apply) {
		this.errMsg_Apply = errMsg_Apply;
		
		// this.jobStatus = XLMgrCons.STATUS_FAIL;
	}

	// cksohn - BULK mode oracle sqlldr
	public String getErrMsg_Loader() {
		return errMsg_Loader;
	}
	public void setErrMsg_Loader(String errMsg_Loader) {
		this.errMsg_Loader = errMsg_Loader;
	}
	
	
	public long getCondCommitCnt() {
		return condCommitCnt;
	}




	public void setCondCommitCnt(long condCommitCnt) {
		this.condCommitCnt = condCommitCnt;
	}

	public long getCondSeq() {
		return condSeq;
	}

	public void setCondSeq(long condSeq) {
		this.condSeq = condSeq;
	}
		

	public String getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}
	
	

	public String getSchedName() {
		return schedName;
	}

	public void setSchedName(String schedName) {
		this.schedName = schedName;
	}
		

	public String getsDate() {
		return sDate;
	}

	public void setsDate(String sDate) {
		this.sDate = sDate;
	}

	
	
	// cksohn - BULK mode oracle sqlldr start - [
	public int getExeMode() {
		return exeMode;
	}

	public void setExeMode(int exeMode) {
		this.exeMode = exeMode;
	}
	

	public String getBulk_pipePath() {
		return bulk_pipePath;
	}


	public void setBulk_pipePath(String bulk_pipePath) {
		this.bulk_pipePath = bulk_pipePath;
	}


	public String getBulk_ctlFilePath() {
		return bulk_ctlFilePath;
	}


	public void setBulk_ctlFilePath(String bulk_ctlFilePath) {
		this.bulk_ctlFilePath = bulk_ctlFilePath;
	}
	// ] - end cksohn - BULK mode oracle sqlldr

	
	// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
	public String getBulk_logFilePath() {
		return bulk_logFilePath;
	}

    // cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
	public void setBulk_logFilePath(String bulk_logFilePath) {
		this.bulk_logFilePath = bulk_logFilePath;
	}
	

	public long getWorkPlanSeq() {
		return workPlanSeq;
	}


	public void setWorkPlanSeq(long workPlanSeq) {
		this.workPlanSeq = workPlanSeq;
	}


	// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
	public boolean isRunLoader() {
		return runLoader;
	}

	// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
	public void setRunLoader(boolean runLoader) {
		this.runLoader = runLoader;
	}
	
	// gssg - xl m2m bulk mode logging 지원
	// gssg - xl o2p bulk mode 지원
	// gssg - ppas bulk mode logging 지원
	public void setApplyCnt(long cnt) { // gssg - xl recvCnt를 applyCnt로 적용
		this.applyCnt = cnt;
	}

	// gssg - xl m2m bulk mode logging 지원
	// gssg - xl o2p bulk mode 지원
	// gssg - ppas bulk mode logging 지원
	public long getApplyCnt() { // gssg - xl recvCnt를 applyCnt로 적용
		return applyCnt;
	}

	// gssg - xl 전체적으로 보완
    // gssg - m2m bulk mode thread 순서 조정
	public boolean isLoadQuery() {
		return loadQuery;
	}

	// gssg - xl 전체적으로 보완
    // gssg - m2m bulk mode thread 순서 조정
	public void setLoadQuery(boolean loadQuery) {
		this.loadQuery = loadQuery;
	}


	// gssg - xl t2t 지원
	// gssg - t2t bulk mode 스레드 순서 조정
	public boolean isWrite() {
		return writeYN;
	}

	// gssg - xl t2t 지원
	// gssg - t2t bulk mode 스레드 순서 조정
	public void setWrite(boolean writePipe) {
		this.writeYN = writePipe;
	}
	
	// gssg - LG엔솔 MS2O
	// gssg - gssg - 공장코드 값 처리
	public String getCustomColname() {
		return customColname;
	}
	public String getCustomValue() {
		return customValue;
	}



	// Thread별 상태 체크 
	public boolean isAliveRecvThread()
	{
		try {
			if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
				// if ( !this.oraRecvThread.isAlive() || !this.oraRecvThread.isInterrupted() || this.errMsg_Recv != null ) {
				if ( !this.oraRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
//			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { //cksohn - xl o2p 기능 추가 - 아직 미개발
//				if ( !this.ppasRecvThread.isAlive() ) {
//					return false;
//				} else {
//					return true;
//				}
				
			} else if ( this.sdbInfo.getDbType() == XLCons.TIBERO ) { // cksohn - xl tibero src 기능 추가

				if ( !this.tiberoRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL) { // gssg - xl m2m 기능 추가
				if ( !this.mySQLRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.sdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB 동기화

				if ( !this.mariaDBRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl p2p 지원
				if ( !this.ppasRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL 분리
				if ( !this.postgreSQLRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms 지원
				if ( !this.mssqlRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Altibase to Altibase 지원
				// gssg - Altibase5 to Altibase7 지원
				if ( !this.altibaseRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.CUBRID ) {
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Cubrid to Cubrid 지원
				if ( !this.cubridRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else {
				XLLogger.outputInfoLog(this.logHead + "[WRAN] SDB type Not found for alive check - " + this.sdbInfo.getDbType());
				return true;
			}
		} catch (Exception e) {
			//XLException.outputExceptionLog(e);
			return false;
		}
	}
	



	public boolean isAliveApplyThread()
	{
		try {
			if ( this.tdbInfo.getDbType() == XLCons.ORACLE ) {
				// if ( !this.oraApplyThread.isAlive() || !this.oraApplyThread.isInterrupted() || this.errMsg_Apply != null) {
				if ( !this.oraApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) { // gssg - xl t2t 지원
				if ( !this.tiberoApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.PPAS ) { // cksohn - xl o2p 기능 추가
				if ( !this.ppasApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL 분리
				if ( !this.postgreSQLApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl m2m 기능 추가
				if ( !this.mySQLApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB 동기화
				if ( !this.mariaDBApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms 지원
				if ( !this.mssqlApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else {
				XLLogger.outputInfoLog(this.logHead + "[WRAN] TDB type Not found for alive check - " + this.tdbInfo.getDbType());
				return true;
			}
		} catch (Exception e) {
			//XLException.outputExceptionLog(e);
			return false;
		}
	}
	
	
	// gssg - stopRecvThread() tdbInfo -> sdbInfo
	public void stopRecvThread()
	{
		try {
			if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
				this.oraRecvThread.interrupt();				
			} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl m2m 기능 추가
				this.mySQLRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB 동기화
				this.mariaDBRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl p2p 지원
				this.ppasRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL 분리
				this.postgreSQLRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms 지원
				this.mssqlRecvThread.interrupt();
			} 
		} catch (Exception e) {
			
		}
	}
	
	public void stopApplyThread()
	{
		try {
			if ( this.tdbInfo.getDbType() == XLCons.ORACLE ) { // cksohn - xl o2p 기능 추가
				this.oraApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) { // gssg - xl t2t 지원
				this.tiberoApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.PPAS ) { // cksohn - xl o2p 기능 추가
				this.ppasApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL 분리
				this.postgreSQLApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl m2m 기능 추가
				this.mySQLApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB 동기화
				this.mariaDBApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms 지원
				this.mssqlApplyThread.interrupt();
			}
		} catch (Exception e) {
			
		}
	}
	
	
	// cksohn - BULK mode oracle sqlldr
	public boolean isAliveRecvBulkThread()
	{
		try {
			if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
				// if ( !this.oraRecvThread.isAlive() || !this.oraRecvThread.isInterrupted() || this.errMsg_Recv != null ) {
				// if ( !this.oraRecvBulkThread.isAlive() ) {
				// cksohn - XL_BULK_MODE_YN - 조건 수정

				// gssg - 한국전파 소스 오라클로 변경
				// gssg - oracle fileThread 개발
			
				if ( this.oraRecvBulkThread== null || !this.oraRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}					
				

				
			} else if ( this.sdbInfo.getDbType() == XLCons.TIBERO ) { // cksohn - xl tibero src 기능 추가
				
				// gssg - thread 순서 조정
							
				if ( this.tiberoRecvBulkThread== null || !this.tiberoRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
								 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl m2m bulk mode 지원
				
				if ( this.mariaDBRecvBulkThread== null || !this.mariaDBRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl o2m 지원
				
				if ( this.mySQLRecvBulkThread== null || !this.mySQLRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl p2p 지원
				
				if ( this.ppasRecvBulkThread== null || !this.ppasRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL 분리
				
				if ( this.postgreSQLRecvBulkThread== null || !this.postgreSQLRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms 지원
				
				if ( this.mssqlRecvBulkThread== null || !this.mssqlRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
				// gssg - 국가정보자원관리원 데이터이관 사업
				// gssg - Altibase to Altibase 지원
				// gssg - Altibase5 to Altibase7 지원
				if ( this.altibaseRecvBulkThread== null || !this.altibaseRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else {
				XLLogger.outputInfoLog(this.logHead + "[WRAN] SDB type Not found for alive check - " + this.sdbInfo.getDbType());
				return true;
			}
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
			return false;
		}
	}
	
	public boolean isAliveApplyBulkThread()
	{
		try {
			if ( this.tdbInfo.getDbType() == XLCons.ORACLE ) {
				// if ( !this.oraApplyThread.isAlive() || !this.oraApplyThread.isInterrupted() || this.errMsg_Apply != null) {
				if ( !this.oraApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) { // gssg - xl t2t 지원
				if ( !this.tiberoApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			}else if ( this.tdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl m2m bulk mode 지원
				if ( !this.mariaDBApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl o2m 지원
				if ( !this.mySQLApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl o2p bulk mode 지원 	// gssg - ppas apply bulk thread 추가
				if ( !this.ppasApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL 분리
				if ( !this.postgreSQLApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms 지원
				if ( !this.mssqlApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			}  else {
				XLLogger.outputInfoLog(this.logHead + "[WRAN] TDB type Not found for alive check - " + this.tdbInfo.getDbType());
				return true;
			}
		} catch (Exception e) {
			XLException.outputExceptionLog(e);
			return false;
		}
	}
	
	
	public void stopRecvBulkThread()
	{
		try {
			
			// this.oraRecvBulkThread.interrupt();
			// cksohn - xl tibero src 기능 추가 start - [ 
			switch ( this.sdbInfo.getDbType()  ) {
			
				case XLCons.ORACLE:
					this.oraRecvBulkThread.interrupt();
					break;
					
				case XLCons.TIBERO:					
					this.tiberoRecvBulkThread.interrupt();
					break;
				
				// gssg - xl m2m bulk mode 지원
				case XLCons.MARIADB:
					this.mariaDBRecvBulkThread.interrupt();
					break;
				
				// gssg - xl o2m 지원
				case XLCons.MYSQL:
					this.mySQLRecvBulkThread.interrupt();
					break;
				
				// gssg - xl p2p 지원
				case XLCons.PPAS:
					this.ppasRecvBulkThread.interrupt();
					break;

				// gssg - xl PPAS/PostgreSQL 분리
				case XLCons.POSTGRESQL:
					this.postgreSQLRecvBulkThread.interrupt();
					break;
					
				// gssg - ms2ms 지원
				case XLCons.MSSQL:
					this.mssqlRecvBulkThread.interrupt();
					break;

			
			}			
			// ] - end cksohn - xl tibero src 기능 추가

			
		} catch (Exception e) {
			
		}
	}
	
	public void stopApplyBulkThread()
	{
		try {
			
			// this.oraApplyBulkThread.interrupt();
			// cksohn - xl tibero src 기능 추가 - tibero target 이 아직 개발되지는 않았으나, 일단 이런 방식으로 수정해 놓자
			switch ( this.tdbInfo.getDbType() ) {
			
				case XLCons.ORACLE:
					this.oraApplyBulkThread.interrupt();
					break;

				// gssg - xl t2t 지원
				case XLCons.TIBERO:
					this.tiberoApplyBulkThread.interrupt();
					break;
					
				// gssg - xl m2m bulk mode 지원
				case XLCons.MARIADB:
					this.mariaDBApplyBulkThread.interrupt();
					break;
				
				// gssg - xl o2m 지원
				case XLCons.MYSQL:
					this.mySQLApplyBulkThread.interrupt();
					break;
				
				// gssg - xl o2p bulk mode 지원
				// gssg - ppas bulk thread 추가
				case XLCons.PPAS:
					this.ppasApplyBulkThread.interrupt();
					break;
					
				// gssg - xl PPAS/PostgreSQL 분리
				case XLCons.POSTGRESQL:
					this.postgreSQLApplyBulkThread.interrupt();
					break;
				
				// gssg - ms2ms 지원
				case XLCons.MSSQL:
					this.mssqlApplyBulkThread.interrupt();
					break;


					
			}			
		} catch (Exception e) {
			
		}
	}




}
