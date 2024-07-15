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
 * �������� ��å  ����
 *
 */
public class XLJobRunPol {
	
	// ��å �⺻ ����
	// ayzn - XLInit ��� ����  - XLJobRunPol : jobseq �ּ�
	//private long 	jobseq = 0;
	private String 	polName = "";
	
	// ayzn - XLInit ��� ����  - XLJobRunPol : �ɼ� grpcode, tableName �߰�
	private String 	grpCode = "";
	private String 	tableName = "";
	private String 	condWhere = ""; // where ����
	
	// ayzn - XLInit ��� ����  - XLJobRunPol : source ���̺��̸�
	private String 	tbSName = "";
	
	private int 	polCommitCnt = 100000; // commit ����
	private String 	cpu_chkpoint = "S"; // S: source only, T:Target only, A: Source & Target 	
	private int 	tmaxJobCnt = 1; // ���̺�(�� ��å��) ���� �ִ� �۾���	
	private int		priority = 1;
	
	private long 	condCommitCnt = 0; // ���ݱ��� ����Ǿ� ������ commit �Ǽ�
	private long 	condSeq = 0; // XL_CONDITION ���̺��� SEQ
	
	
	private String	schedName = "";
	
	// src db info
	private XLDBMSInfo sdbInfo = null;
	// tar db info
	private XLDBMSInfo tdbInfo = null;
	
	// src & tar ���̺� ����
	private XLJobTableInfo tableInfo = null;
	
	// sql ���� ���� ����
	private String 	idxHint = ""; // �ҽ� ���� - delete�� ���
	private int 	parallel= 1; // parallel hint ��
	private boolean orderByYN = false; 
	
	private String 	srcSelectSql = "";
	private String 	tarInsertSql = "";
	
	
	// JOB ���� ���� �ð� yyyy-mm-dd HH24:MM:SS
	private String sDate = "";
	
	
	// cksohn - BULK mode oracle sqlldr start - [
	// private int		exeMode 	= XLCons.BULK_MODE; // ���� ��å���� mode�� ���� ���� �ʿ�
	// cksohn - XL_CAP_READ_REDO_YN - switching �� ������ ���� ���� - sender check
	private int		exeMode 	= XLOGCons.NORMAL_MODE; // ���� ��å���� mode�� ���� ���� �ʿ�
	
	private String	bulk_pipePath = "";
	private String	bulk_ctlFilePath = "";
	
	
	// cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
	private String	bulk_logFilePath = "";
	
	// ] - end
	
	// gssg - LG���� MS2O
	// gssg - gssg - �����ڵ� �� ó��
	String customColname = "";
	String customValue = "";
	
	
	// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
	// sqlldr�� ���࿩��/���� ����
	private boolean	runLoader = false;
	
	// gssg - xl ��ü������ ����
    // gssg - m2m bulk mode thread ���� ����
	private boolean loadQuery = false;
	
	// gssg - xl t2t ����
	// gssg - t2t bulk mode ������ ���� ����
	// recvBulk �������� ���࿩��
	private boolean writeYN = false;
	
	// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
	private long workPlanSeq = 0;
	
	//#######################################################
	// �������� ��å(JOB)�� Recv Thread�� Apply Thread�� ������ ���� Q
	//#######################################################
	private XLDataQ dataQ = new XLDataQ(); 
	
	//#################################################
	// DBMS Ÿ�Ժ��� ���� ��� ������ �ֵ���. or ���� �ٸ� ��� �ʿ�
	//#################################################
	// ORACLE
	private XLOracleRecvThread oraRecvThread = null;
	private XLOracleApplyThread oraApplyThread = null;
		
	// cksohn - BULK mode oracle sqlldr
	private XLOracleRecvBulkThread oraRecvBulkThread = null;
	private XLOracleApplyBulkThread oraApplyBulkThread = null;
	
	// gssg - SK��Ʈ�� O2O
	// gssg - linkMode ����
	private XLOracleLinkThread oraLinkThread = null;
	
	// gssg - �ѱ����� �ҽ� ����Ŭ�� ����
	// gssg - oracle fileThread ����
	// ayzn - XLInit ��� ����  - XLJobRunPol : init���� ��������ʴ� file������� �ּ�
	//private XLOracleRecvBulkFileThread oraRecvBulkFileThread = null;
	
	// gssg - xl m2m ���� ���� ���� - TODO
	// MySQL
	// gssg - xl m2m ��� �߰�
	private XLMySQLRecvThread mySQLRecvThread = null;
	private XLMySQLApplyThread mySQLApplyThread = null;
	
	// gssg - xl o2m ����
	private XLMySQLRecvBulkThread mySQLRecvBulkThread = null;
	private XLMySQLApplyBulkThread mySQLApplyBulkThread = null;
	
	// gssg - xl m2m ��� �߰�  - 0413
	// MariaDB
	private XLMariaDBRecvThread mariaDBRecvThread = null;
	private XLMariaDBApplyThread mariaDBApplyThread = null;
	
	// gssg - xl m2m bulk mode ����
	private XLMariaDBRecvBulkThread mariaDBRecvBulkThread = null;
	private XLMariaDBApplyBulkThread mariaDBApplyBulkThread = null;
			
	// cksohn - xl o2p ��� �߰� start - [
	// PPAS
	// private XLIMPPASRecvThread ppasRecvThread = null; // ���� �̰���
	// gssg - xl o2p bulk mode ����
	// gssg - ppas apply bulk thread �߰�
	// gssg - xl p2p ����
	private XLPPASRecvThread ppasRecvThread = null;
	private XLPPASApplyThread ppasApplyThread = null;
	
	private XLPPASRecvBulkThread ppasRecvBulkThread = null;
	private XLPPASApplyBulkThread ppasApplyBulkThread = null;
	
	// - end 
	
	// gssg - xl PPAS/PostgreSQL �и�
	private XLPostgreSQLRecvThread postgreSQLRecvThread = null;
	private XLPostgreSQLApplyThread postgreSQLApplyThread = null;
	
	private XLPostgreSQLRecvBulkThread postgreSQLRecvBulkThread = null;
	private XLPostgreSQLApplyBulkThread postgreSQLApplyBulkThread = null;
		
	// cksohn - xl tibero src ��� �߰� start - [
	private XLTiberoRecvThread tiberoRecvThread = null;
	private XLTiberoRecvBulkThread tiberoRecvBulkThread = null;
	// ] - end cksohn - xl tibero src ��� �߰�
	// gssg - xl t2t ����
	private XLTiberoApplyThread tiberoApplyThread = null;
	private XLTiberoApplyBulkThread tiberoApplyBulkThread = null;
	
	// gssg - csv file create ��� �߰�
	// gssg - file create Ŭ���� �и�					
	
		
	// gssg - ms2ms ����
	private XLMSSQLRecvThread mssqlRecvThread = null;
	private XLMSSQLApplyThread mssqlApplyThread = null;
	
	private XLMSSQLRecvBulkThread mssqlRecvBulkThread = null;
	private XLMSSQLApplyBulkThread mssqlApplyBulkThread = null;
	
	// gssg - cubrid support
	private XLCubridRecvThread cubridRecvThread = null;
	private XLCubridApplyThread cubridApplyThread = null;
	
	// gssg - ���������ڿ������� �������̰� ���
	// gssg - Altibase to Altibase ����
	private XLAltibaseRecvThread altibaseRecvThread = null;
	private XLAltibaseApplyThread altibaseApplyThread = null;
	// gssg - Altibase to Oracle ����
	private XLAltibaseRecvBulkThread altibaseRecvBulkThread = null;
	
	
	// job ����
	private String jobStatus = XLOGCons.STATUS_RUNNING;
	
	// �����庰 ���� 
	private String errMsg_Recv = null;
	private String errMsg_Apply = null;
	
	// cksohn - BULK mode oracle sqlldr
	private String errMsg_Loader = null;
	
	// gssg - xl m2m bulk mode logging ����
	private long applyCnt = 0; // gssg - xl recvCnt�� applyCnt�� ����
	

	// ����� ���� �Ǵ� ������ ����� stopFlag false --> true
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
	
	// ��å���� ����
	// public boolean makeInfo()
	public boolean makeInfo(Connection _cataConn)
	{
		try {
			
			
			XLMDBManager mDBMgr = new XLMDBManager();
			
			// ayzn - XLInit ��� ����  - XLJobRunPol : commit_count, parallel �ɼ� ó�� �߰�
			if(XLConf.XL_COMMIT_COUNT>0)
			{
				this.polCommitCnt = XLConf.XL_COMMIT_COUNT;
			}	
			
			if(XLConf.XL_PARALLEL>1)
			{
				this.parallel = XLConf.XL_PARALLEL;
			}	
			
			// ayzn - XLInit ��� ����  - XLJobRunPol :  ���� ��� ���� ���� (Source, Target)
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
			
			// 1. �����ؾ� �� JOB�� ���� ������ DB�� ���� �����Ͽ� setting �Ѵ�.
			// ayzn - XLInit ��� ����  - XLJobRunPol : getJobRunPolInfo �Լ�ó�� �� ������ �߰� �� ����
			//Vector vt = mDBMgr.getJobRunPolInfo(_cataConn, this.jobseq);
			Vector vt = mDBMgr.getJobRunPolInfo(_cataConn, this.grpCode, this.polName, dicOwner, dicTname);
			
			XLLogger.outputInfoLog("getJobRunPolInfo"); 
			XLLogger.outputInfoLog(vt);
			
			// DBMS ���� ����
			if ( vt == null ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Job - " + this.polName + "/" + condWhere);
				return false;
			}
			
			if ( vt.size() == 0 ) {
				XLLogger.outputInfoLog("[WARN] Failed to Run Job [NO Information] - " + this.polName + "/" + condWhere);
				return false;
				
			}
			XLLogger.outputInfoLog("##################  getJobRunPolInfo select  ####################");			
			
			// ���̺� �� �÷� ����			
			for ( int i=0; i<vt.size(); i++ ) {
				
				Hashtable ht = (Hashtable)vt.get(i);
				
				if ( i == 0 ) { // pol ������ �ѹ��� ���ָ� �ȴ�.

					
					// pol ����
					// ayzn - XLInit ��� ����  - XLJobRunPol : CDC īŻ�α� ���̺� ���� ������ �ּ�ó��
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
					
					this.condWhere = (String)ht.get("CONDITION_WHERE"); // ���⼭ �ѹ� �� ����
					
					// gssg - �Ϻ� ��Ʈ��ũ �л� ó��
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
					
					// src & tar db ����
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
					
					// gssg - SK��Ʈ�� O2O -- start
					// gssg -  FROM ���� ����ڰ� ������ �� �״�� ���� ��� ����
					// ayzn - XLInit ��� ����  - XLJobRunPol : CDC īŻ�α� ���̺� ���� ������ �ּ�ó��
					//	String selectScript = (String)ht.get("POL_SELECT_SCRIPT");
					String selectScript = "";
					
					// gssg - linkMode ����
					// ayzn - XLInit ��� ����  - XLJobRunPol : CDC īŻ�α� ���̺� ���� ������ �ּ�ó��
					//String dblinkName = (String)ht.get("POL_DBLINK_NAME");
					String dblinkName = "";
					
					

					this.tableInfo = new XLJobTableInfo(sOwner, sTable, tOwner, tTable, selectScript, dblinkName);
					
					// gssg - SK��Ʈ�� O2O -- end
					
					
				
				}
				
				
				// 3. Table �÷��� ���� ����
				// ayzn - XLInit ��� ����  - XLJobRunPol : Table �÷� ���� ���� �� CDC īŻ�α� ���̺� ������ ����
				String colName =  (String)ht.get("DIC_COLNAME");
				String colNameMap = (String)ht.get("DIC_COLNAME_MAP");
				String dataTypeStr = (String)ht.get("DIC_DATATYPE");				
				int dataType = XLDicInfo.convertDataType(dataTypeStr);
				
				int colId = Integer.parseInt((String)ht.get("DIC_COLID"));

				String logmnrYN = (String)ht.get("DIC_LOGMNR_YN");
						
				// gssg - xl function ��� ����
				// gssg - dicinfo_function �÷� �߰�
				String functionStr = (String)ht.get("DIC_FUNCTION");

				/// gssg - �ҽ� Function ��� �߰�
				String functionStrSrc = (String)ht.get("DIC_FUNCTION");
				
				
				// gssg - o2o damo ����
				// gssg - dicinfo_sec_yn �÷� �߰�
				// ayzn - XLInit ��� ����  - XLJobRunPol : CDC īŻ�α� ���̺� ���� ������ �ּ�ó��
				//String secYN = (String)ht.get("DICINFO_SEC_YN");
				//String secMapYN = (String)ht.get("DICINFO_SEC_MAP_YN");
				String secYN = "";
				String secMapYN = "";
				// gssg - �ҽ� Function ��� �߰�
				XLJobColInfo colInfo = new XLJobColInfo(colName, colNameMap, dataType, colId, logmnrYN, functionStr, functionStrSrc, secYN, secMapYN);
				this.tableInfo.addColInfo(colInfo);
				
			} // for-end
			
			
			
			
			// gssg - LG���� MS2O
			// gssg - �����ڵ� �� ó�� -- start 
			/*
			 * Vector customVt = mDBMgr.getCustomCode(polName); if ( customVt.size() != 0 )
			 * { Hashtable customHt = (Hashtable)customVt.get(0); this.customColname =
			 * (String)customHt.get("DICINFO_COLNAME"); this.customValue =
			 * (String)customHt.get("DICCODE_VALUE"); }
			 */				
									
			// -- end
			
			// gssg - xl p2t ����
			// gssg - p2t �ϴٰ� lob check ����
			if( sdbInfo.getDbType() == XLCons.ORACLE ) {
				// gssg - o2t ����
				// gssg - interval type ����ó��
				if ( tdbInfo.getDbType() == XLCons.TIBERO ) {
					this.tableInfo.checkTiberoTypeYN();					
				} else if ( tdbInfo.getDbType() == XLCons.ORACLE ) {
					// gssg - ���������ڿ������� �������̰� ���
					// gssg - Oracle to Oracle Ÿ���� ó��
					this.tableInfo.checkOraToOraTypeYN();					
				} else {
					this.tableInfo.checkOraTypeYN();
				}
			} else if( sdbInfo.getDbType() == XLCons.MYSQL || sdbInfo.getDbType() == XLCons.MARIADB ) {
				this.tableInfo.checkMySQLTypeYN();
			} else if( sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.TIBERO ) {
				this.tableInfo.checkTiberoTypeYN();
			} else if( sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.ORACLE ) {
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Tibero to Oracle Ÿ���� ó��
				this.tableInfo.checkOraToOraTypeYN();
			} 
			// gssg - xl ��ü������ ����2
			// gssg - PostgreSQL Ŀ���� ó��
			else if( (sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.PPAS) || 
					(sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.POSTGRESQL) ) {
				// gssg - o2m �ϴٰ� t2p bulk mode ����
				this.tableInfo.checkTiberoTypeYN();
			}
			// gssg - ���������ڿ������� �������̰� ���
			// gssg - t2m bulk mode ����
			else if( (sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.MYSQL) || 
					(sdbInfo.getDbType() == XLCons.TIBERO && tdbInfo.getDbType() == XLCons.MARIADB) ) {
				// gssg - o2m �ϴٰ� t2p bulk mode ����
				this.tableInfo.checkTiberoTypeYN();
			}
			// gssg - xl ��ü������ ����2
			// gssg - PostgreSQL Ŀ���� ó��
			else if( sdbInfo.getDbType() == XLCons.PPAS || sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
				// gssg - o2m �ϴٰ� p2p bulk mode ����
				this.tableInfo.checkPPASTypeYN();
			}
			// gssg - ���������ڿ������� �������̰� ���
			// gssg - Altibase to Oracle ����
			// gssg - Altibase5 to Altibase7 ����
			else if( sdbInfo.getDbType() == XLCons.ALTIBASE || sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
				this.tableInfo.checkOraTypeYN();
			}
			// gssg - LG���� MS2O
			// gssg - ms2ms normal mode ����
			else if( sdbInfo.getDbType() == XLCons.MSSQL ) {
				this.tableInfo.checkMSSQLTypeYN();
			}

			// cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ��� start - [
			// if ( XLConf.XL_BULK_MODE_YN ) {
			// gssg - SK��Ʈ�� O2O
			// gssg - linkMode ����
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

			// ] - end cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ���
			
			XLLogger.outputInfoLog(this.logHead + "make information is finished - " + this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
						
			// 4. ���� ���� ����
			if ( !makeQuery() ) {
				// ���� ���� ����
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

	// �ҽ� select & target insert PreparedStatement ���� ����
	private boolean makeQuery()
	{
		
		//XLLogger.outputInfoLog("#### makeQuert Called!!!!!!!!!!!!!!!!!!!!!!!");
		
		try {
			
			XLLogger.outputInfoLog("# makeQuery start #");
			StringBuffer sb = new StringBuffer();
			
			
			// gssg - ���������ڿ������� �������̰� ���
			// gssg - refactoring
//			StringBuffer lobSb = new StringBuffer();

			Vector<XLJobColInfo> vtColInfo = this.tableInfo.getVtColInfo();			
			
			// orderBy�� ����
			Vector<String> vtOrderByCol = new Vector<String>();
			
			// 1. Src select ���� ����
			// gssg - SK��Ʈ�� O2O -- start
			// gssg - linkMode ����
			XLLogger.outputInfoLog("# parallel #" + this.parallel);
			XLLogger.outputInfoLog("# idxHint #" + this.idxHint);
			if ( this.tableInfo.getDblinkName() == null || this.tableInfo.getDblinkName().equals("") ) 
			{

				// gssg -  SELECT ���� ����ڰ� ������ �� �״�� ���� ��� ����
				if ( this.tableInfo.getSelectScript() == null || this.tableInfo.getSelectScript().equals("") ) {
				
					sb.append("SELECT ");
					if ( this.parallel > 1) {
						sb.append(" /*+ PARALLEL(").append(this.tableInfo.getStable()).append(" ").append(this.parallel).append(") */ ");
					}
					
					// TODO : indexHint ����� !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
					if ( this.idxHint != null && !this.idxHint.equals("") ) {
						sb.append(this.idxHint + " ");
					}
										
					
					// 1-1 select column
					
					for ( int i=0; i<vtColInfo.size(); i++) {
						// gssg -xl lob Ÿ�� ����
						// gssg - LOB �÷� ���� ������ ó��
//							if ( i !=0 ) {
//								sb.append(",");
//							}					

						XLJobColInfo colInfo = vtColInfo.get(i);
			
						// sb.append(colInfo.getColName());
						// cksohn - xlim LOB Ÿ�� ����			
						// gssg - t2o LOB �÷� ���� ������ ó��
						if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
						// cksohn - xl tibero src ��� �߰�
						// if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO) {
							
							if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {						
								// sb.append("e." + colInfo.getColName() + ".getClobVal()");
								// cksohn - xl tibero src ��� �߰�
								// sb.append("e." + colInfo.getColName() + ".getClobVal() AS " +  colInfo.getColName());
								
								// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
//								sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");
								
								// gssg -xl lob Ÿ�� ����
								// gssg - LOB �÷� ���� ������ ó��
								if ( i !=0 ) {
									// gssg - ���������ڿ������� �������̰� ���
									// gssg - refactoring
//									lobSb.append(",");
									sb.append(",");
								}					
								if( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
																							
									// gssg - �ҽ� Function ��� �߰�
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										// gssg - ���������ڿ������� �������̰� ���
										// gssg - refactoring												
																		
//										lobSb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );
										sb.append( colInfo.getFunctionStrSrc() + "(" + "e.\"" + colInfo.getColName() + "\".getClobVal()" + ")" + " AS \"" +  colInfo.getColName() + "\"" );

									} else {
										 // Tibero������ �Ʒ� getClobVal()���� ���� �߻���
										// gssg - ���������ڿ������� �������̰� ���
										// gssg - refactoring
//										lobSb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															
										sb.append("e.\"" + colInfo.getColName() + "\".getClobVal() AS \"" +  colInfo.getColName() + "\"");															

									}
									
								} else {
									// gssg - �ҽ� Function ��� �߰�
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										// gssg - ���������ڿ������� �������̰� ���
										// gssg - refactoring
//										lobSb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
										sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );

									} else {
										// gssg - ���������ڿ������� �������̰� ���
										// gssg - refactoring
//										lobSb.append("\"" + colInfo.getColName() + "\"");								
										sb.append("\"" + colInfo.getColName() + "\"");								
									}
								}
										
					
							// cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE
							} 
											
							else if ( colInfo.getDataType() == XLDicInfoCons.CHAR ||
											colInfo.getDataType() == XLDicInfoCons.VARCHAR2 ) {
								
								// gssg -xl lob Ÿ�� ����
								// gssg - LOB �÷� ���� ������ ó��
								if ( i !=0 ) {
									sb.append(",");
								}
								
								if ( XLConf.XL_SRC_CHAR_RAWTOHEX_YN ) {
									// sb.append("RAWTOHEX(" + colInfo.getColName() + ")");
									// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
									sb.append("RAWTOHEX(\"" + colInfo.getColName() + "\")");
								} else {			
									
									// gssg - �ҽ� Function ��� �߰�
									if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
										sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
									} else {
										sb.append("\"" + colInfo.getColName() + "\"");								
									}
									
									// sb.append(colInfo.getColName());
									// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
//									sb.append("\"" + colInfo.getColName() + "\"");
								}
				
							} else {
								
								if ( i !=0 ) {
									sb.append(",");
								}
														
								// gssg - �ҽ� Function ��� �߰�
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}

								// sb.append(colInfo.getColName());
								// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
//								sb.append("\"" + colInfo.getColName() + "\"");
								
							}
							
						} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL ||  this.sdbInfo.getDbType() == XLCons.MARIADB ) {
							
							// gssg - ���������ڿ������� �������̰� ���
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
							
							// gssg - xl p2p ����
							// gssg - p2p LOB �÷� ���� ������ ó��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							if ( i != 0 ) {
								sb.append(",");
							}
							
							// gssg - �ҽ� Function ��� �߰�
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - PostgreSQL to PostgreSQL ����
							if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
//								sb.append( colInfo.getFunctionStrSrc() + "(" + colInfo.getColName() + ")" );
								sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
							} else {
//								sb.append( colInfo.getColName() );								
								sb.append("\"" + colInfo.getColName() + "\"");								
							}		
											
						} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
							
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - Altibase to Oracle ����
							// gssg - Altibase5 to Altibase7 ����
								if ( i != 0 ) {
									sb.append(",");
								}
								
								// gssg - �ҽ� Function ��� �߰�
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}
							
						} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) {
							
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - Altibase to Oracle ����
							// gssg - Altibase5 to Altibase7 ����
								if ( i != 0 ) {
									sb.append(",");
								}
								
								// gssg - �ҽ� Function ��� �߰�
								if ( colInfo.getFunctionStrSrc() != null && !colInfo.getFunctionStrSrc().equals("") ) {
									sb.append( colInfo.getFunctionStrSrc() + "(" + "\"" + colInfo.getColName() + "\"" + ")" );
								} else {
									sb.append("\"" + colInfo.getColName() + "\"");								
								}
							
						} else {
							
							// gssg -xl lob Ÿ�� ����
							// gssg - LOB �÷� ���� ������ ó��
							if ( i !=0 ) {
								sb.append(",");
							}
							
							// gssg - �ҽ� Function ��� �߰�
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
					
					// gssg - ���������ڿ������� �������̰� ���
					// gssg - refactoring
//					sb.append(lobSb);
						
					sb.append(" FROM ");
					// sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
					// cksohn - xlim LOB Ÿ�� ����
					// if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
					// cksohn - xl tibero src ��� �߰�
					// gssg - ���������ڿ������� �������̰� ���
					// gssg - tibero tsn ���� ��� �߰�
					if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
						// sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable() + " e");
						
						// cksohn - XL_INIT_SCN start - [
						if ( XLConf.XL_INIT_SCN != 0 ) {
							
							// cksohn - XL_INIT_SCN - SCN ������ ���̺� alias e ������ Syntax Error ����
							// gssg - o2o damo ����
							// gssg - as of scn ��ҹ��� ���� ó��
//							sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable());
							sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"");

							sb.append(" AS OF SCN " + XLConf.XL_INIT_SCN);
							sb.append(" e");
							
						} else { // cksohn - XL_INIT_SCN - SCN ������ ���̺� alias e ������ Syntax Error ����
							
							// sb.append(this.tableInfo.getSowner() + "." + this.tableInfo.getStable() + " e");
							// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
							sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");

						}
					
					} else if (this.sdbInfo.getDbType() == XLCons.MYSQL || this.sdbInfo.getDbType() == XLCons.MARIADB) { // gssg - xl MySQL src ��� �߰�
						// gssg - xl m2m ��� �߰�
						sb.append("`" + this.tableInfo.getSowner() + "`" + "." + "`" + this.tableInfo.getStable() + "`" + " e");
						
					} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) { 				
						// gssg - ���������ڿ������� �������̰� ���
						// gssg - Altibase to Oracle ����
						// gssg - Altibase5 to Altibase7 ����
						sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");
						
					} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL || this.sdbInfo.getDbType() == XLCons.PPAS ) { 				
						// gssg - ���������ڿ������� �������̰� ���
						// gssg - PostgreSQL to PostgreSQL ����
						sb.append("\"" + this.tableInfo.getSowner() + "\"" + "." + "\"" + this.tableInfo.getStable() + "\"" + " e");
						
					} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) {
						// gssg - SK�ڷ��� O2M, O2P
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
				
					// orderBy �� ����
					if (vtOrderByCol.size() > 0) {
						sb.append(" ORDER BY ");
						for (int i=0; i<vtOrderByCol.size(); i++) {
							if ( i != 0 ) {
								sb.append(",");
							}
							// sb.append(vtOrderByCol.get(i));
							// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
							if (this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							} 
							// gssg - xl ��ü������ ����
							// gssg - m2m ��ҹ��� ó��
							else if ( this.sdbInfo.getDbType() == XLCons.MYSQL || this.sdbInfo.getDbType() == XLCons.MARIADB ) {
								sb.append("`" + vtOrderByCol.get(i) + "`");						
							}
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - Altibase to Oracle ����
							// gssg - Altibase5 to Altibase7 ����
							else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							}
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - PostgreSQL to PostgreSQL ����						
							else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL || this.sdbInfo.getDbType() == XLCons.PPAS ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							}
							// gssg - SK�ڷ��� O2M, O2P
							else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) {
								sb.append("\"" + vtOrderByCol.get(i) + "\"");
							}
							else {
								sb.append(vtOrderByCol.get(i));
							}
						}
							
					}			
					// gssg - SK��Ʈ�� O2O
					// gssg - alias ����
					this.srcSelectSql = sb.toString();
					this.srcSelectSql = srcSelectSql.replace("XL_MGR_ALIAS", XLConf.XL_MGR_ALIAS);
					
					
					// 2. Tar insert PreparedStatement ���� ����
					sb = new StringBuffer();
					
					// gssg - ���������ڿ������� �������̰� ���
					// gssg - refactoring
//					lobSb = new StringBuffer();
					
					sb.append("INSERT INTO ");
					
					
					// sb.append(this.tableInfo.getTowner() + "." + this.tableInfo.getTtable());
					// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����			
					if (this.tdbInfo.getDbType() == XLCons.ORACLE || this.tdbInfo.getDbType() == XLCons.TIBERO ) {
						// gssg - SK�ڷ��� O2M, O2P -- start
						// sb.append("\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"");
						if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
							sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );							
						} else {
							sb.append( this.tableInfo.getTowner() + "." + this.tableInfo.getTtable() );
						}
						// gssg - SK�ڷ��� O2M, O2P -- end						

					} 
					// gssg - xl m2m ���� ���� ����
					// gssg - xl m2m ��� �߰�
					else if(this.tdbInfo.getDbType() == XLCons.MYSQL || this.tdbInfo.getDbType() == XLCons.MARIADB ) {					
						
						 sb.append("`" + this.tableInfo.getTowner() + "`" + "." + "`" + this.tableInfo.getTtable() + "`");						
						
					}
					// gssg - xl ��ü������ ����
					// gssg - o2p ��ҹ��� ó��
					// gssg - xl ��ü������ ����2
					// gssg - PostgreSQL Ŀ���� ó��
					else if( this.tdbInfo.getDbType() == XLCons.PPAS || this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) {
						// gssg - SK�ڷ��� O2M, O2P -- start
						// sb.append("\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"");
						if ( this.sdbInfo.getDbType() == XLCons.PPAS || this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
							sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );							
						} else {
							sb.append( this.tableInfo.getTowner() + "." + this.tableInfo.getTtable() );
						}
						// gssg - SK�ڷ��� O2M, O2P -- end
						
					}									
					// gssg - ���������ڿ������� �������̰� ���
					// gssg - Altibase to Oracle ����
					// gssg - Altibase5 to Altibase7 ����
					else if( this.tdbInfo.getDbType() == XLCons.ALTIBASE || this.tdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
						// gssg - SK�ڷ��� O2M, O2P -- start						
						// sb.append("\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"");
						if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
							sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );							
						} else {
							sb.append( this.tableInfo.getTowner() + "." + this.tableInfo.getTtable() );							
						}
						// gssg - SK�ڷ��� O2M, O2P -- end
					}
					// gssg - SK�ڷ��� O2M, O2P -- start
					else if( this.tdbInfo.getDbType() == XLCons.MSSQL ) {
						
						sb.append( "\"" + this.tableInfo.getTowner() + "\"" + "." + "\"" + this.tableInfo.getTtable() + "\"" );
												
					}
					// gssg - SK�ڷ��� O2M, O2P -- end
					else {
						sb.append(this.tableInfo.getTowner() + "." + this.tableInfo.getTtable());
					}						
					// insert columns
					sb.append("(");
					
					// gssg - LG���� MS2O
					// gssg - �����ڵ� �� ó�� -- start 
					if ( this.customColname != null && !this.customColname.equals("") ) {
						sb.append("\"" + this.customColname + "\",");				
					}
					// --end

					
					for (int i=0; i<vtColInfo.size(); i++) {

						// gssg -xl lob Ÿ�� ����
						// gssg - LOB �÷� ���� ������ ó��
//							if ( i != 0 ) {
//								sb.append(",");
//							}
						
						XLJobColInfo colInfo = vtColInfo.get(i);
						// sb.append(colInfo.getColName_map());
						// cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����
						
						// gssg -xl lob Ÿ�� ����
						// gssg - o2o LOB �÷� ���� ������ ó��
						if ( this.tdbInfo.getDbType() == XLCons.ORACLE || this.tdbInfo.getDbType() == XLCons.TIBERO ) {

//							
//							sb.append("\"" + colInfo.getColName_map() + "\"");

							// gssg - ���������ڿ������� �������̰� ���
							// gssg - refactoring
							if ( i != 0 ) {
								sb.append(",");
							}
							// gssg - SK�ڷ��� O2M, O2P -- start
							// sb.append("\"" + colInfo.getColName_map() + "\"");
							if ( this.sdbInfo.getDbType() == XLCons.ORACLE || this.sdbInfo.getDbType() == XLCons.TIBERO ) {
								sb.append( "\"" + colInfo.getColName_map() + "\"" );								
							} else {
								sb.append( colInfo.getColName_map() );
							}
							// gssg - SK�ڷ��� O2M, O2P -- end										
						} else if ( this.tdbInfo.getDbType() == XLCons.PPAS || this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) {
		                     
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - PostgreSQL to PostgreSQL ����						
							if ( i != 0 ) {
								sb.append(",");
							}
							// gssg - SK�ڷ��� O2M, O2P -- start
							// sb.append("\"" + colInfo.getColName_map() + "\"");
							if ( this.sdbInfo.getDbType() == XLCons.PPAS || this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
								sb.append( "\"" + colInfo.getColName_map() + "\"" );								
							} else {
								sb.append( colInfo.getColName_map() );
							}
							// gssg - SK�ڷ��� O2M, O2P -- end

		                  }  else if ( this.tdbInfo.getDbType() == XLCons.MYSQL || this.tdbInfo.getDbType() == XLCons.MARIADB ) {
							
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - refactoring
							if ( i != 0 ) {
								sb.append(",");
							}
								sb.append("`" + colInfo.getColName_map() + "`");																					
																													
						} else if ( this.tdbInfo.getDbType() == XLCons.ALTIBASE || this.tdbInfo.getDbType() == XLCons.ALTIBASE5 ) {

							// gssg - ���������ڿ������� �������̰� ���
							// gssg - Altibase to Oracle ����				
							// gssg - Altibase5 to Altibase7 ����
							if ( i != 0 ) {
								sb.append(",");
							}
							// gssg - SK�ڷ��� O2M, O2P -- start
							// sb.append("\"" + colInfo.getColName_map() + "\"");
							if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
								sb.append( "\"" + colInfo.getColName_map() + "\"" );								
							} else {
								sb.append( colInfo.getColName_map() );
							}
							// gssg - SK�ڷ��� O2M, O2P -- end			
									
						} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) {
							
							if ( i != 0 ) {
								sb.append(",");
							}
							sb.append("\"" + colInfo.getColName_map() + "\"");							

						} else {
							// gssg -xl lob Ÿ�� ����
							// gssg - LOB �÷� ���� ������ ó��
							if ( i != 0 ) {
								sb.append(",");
							}				
							sb.append(colInfo.getColName_map());
						}

					}
					
					// gssg -xl lob Ÿ�� ����
					// gssg - LOB �÷� ���� ������ ó��
//					sb.append(lobSb);
					
					sb.append(")");
					
					// values ?
					sb.append(" VALUES (");
					
					
					// gssg - LG���� MS2O
					// gssg - �����ڵ� �� ó�� -- start 
					if ( this.customValue != null && !this.customValue.equals("") ) {
						sb.append("'" + this.customValue + "',");				
					}
					// --end

					
					for (int i=0; i<vtColInfo.size(); i++) {
						if ( i != 0 ) {
							sb.append(",");
						}
						
						// sb.append("?");
						// cksohn - xlim LOB Ÿ�� ����
						XLJobColInfo colInfo = vtColInfo.get(i);
						// if ( this.sdbInfo.getDbType() == XLCons.ORACLE ) {
						if ( this.tdbInfo.getDbType() == XLCons.ORACLE ) {
							if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE ) {
								// gssg - ����� O2O
								// gssg - raw_to_varchar2 ��� ����
								if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
									sb.append("sys.XMLType.createXML(" + colInfo.getFunctionStr() + ")");
								} else {
									sb.append("sys.XMLType.createXML(?)");	
								}									
								
							} else {						
								// gssg - xl function ��� ����
								// gssg - insert values �κп� �Լ� �����						
								if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
									// gssg - LG���� MS2O
									// gssg - function �Ķ���� ������
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
								// gssg - xl function ��� ����
								// gssg - insert values �κп� �Լ� �����						
								if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {									

									// gssg - �����Ϸ��� O2MS
									if ( colInfo.getFunctionStr().contains("?") ) {
										sb.append(colInfo.getFunctionStr());										
									} else {
										// Ƽ���ο����� "lower"(�÷�) �� ���� �����Լ��� �ֵ���ǥ(")�� ���� �� ����
										sb.append( colInfo.getFunctionStr() + "(?)");										
									}									
								} else {
									sb.append("?");							
								}					
						} else if ( this.tdbInfo.getDbType() == XLCons.PPAS || 
								this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // cksohn - xl o2p ��� �߰�
						
								// gssg - xl ��ü������ ����2
								// gssg - PostgreSQL Ŀ���� ó��
								if ( colInfo.getDataType() == XLDicInfoCons.XMLTYPE || colInfo.getDataType() == XLDicInfoCons.XML ) {						
									sb.append("XML(?)");
								} else if ( colInfo.getDataType() == XLDicInfoCons.INTERVAL_DS || colInfo.getDataType() == XLDicInfoCons.INTERVAL_YM ) {
									
									sb.append(" CAST(?  AS INTERVAL)");
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.BITVARYING ) {
																
									// gssg - xl p2p ����
									// gssg - p2p normal mode Ÿ�� ó��
									sb.append("(?)::VARBIT");
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.BIT ) {
									
									// gssg - xl p2p ����
									// gssg - p2p normal mode Ÿ�� ó��
									sb.append("(?)::VARBIT");
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.MONEY ) {
									
									// gssg - xl ��ü������ ����2
									// gssg - PostgreSQL Ŀ���� ó��
									if ( this.sdbInfo.getDbType() == XLCons.PPAS || 
											this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
										sb.append(" CAST(?  AS MONEY)");
									} else {
										sb.append("?");
									}
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.TIME || colInfo.getDataType() == XLDicInfoCons.TIME_TZ ) {
									
									// gssg - xl ��ü������ ����2
									// gssg - PostgreSQL Ŀ���� ó��
									if ( this.sdbInfo.getDbType() == XLCons.PPAS || 
											this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
										sb.append(" CAST(?  AS TIME)");
									} else {
										sb.append("?");
									}
									
								} else if ( colInfo.getDataType() == XLDicInfoCons.NUMERIC || colInfo.getDataType() == XLDicInfoCons.DECIMAL ) {
									
									// gssg - ��� ����
									// gssg - P2P - normal NUMERIC Ÿ�� ����
									if ( this.sdbInfo.getDbType() == XLCons.PPAS || 
											this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) {
										sb.append(" CAST(?  AS NUMERIC)");
									} else {
										sb.append("?");
									}
									
								}  else {							
									// gssg - xl function ��� ����
									// gssg - insert values �κп� �Լ� �����						
									if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {										
										// gssg - �����Ϸ��� O2MS
										if ( colInfo.getFunctionStr().contains("?") ) {
											sb.append(colInfo.getFunctionStr());										
										} else {
											// gssg - SK�ڷ��� O2M, O2P
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

							// gssg - xl m2m ��� �߰� 
							// gssg - xl function ��� ����
							// gssg - insert values �κп� �Լ� �����						
							if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
								// gssg - �����Ϸ��� O2MS
								if ( colInfo.getFunctionStr().contains("?") ) {
									sb.append(colInfo.getFunctionStr());										
								} else {
									sb.append( colInfo.getFunctionStr() + "(?)");										
								}									

							} else {
								sb.append("?");							
							}
						} else {					
							// gssg - ms2ms ����
							// gssg - function ����
							if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
								
								// gssg - �����Ϸ��� O2MS
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
			
			// gssg - SK��Ʈ�� O2O
			// gssg - alias ����
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

			// gssg - SK��Ʈ�� O2O
			// gssg - alias ����
			this.srcSelectSql = srcSelectSql.replace("XL_MGR_ALIAS", XLConf.XL_MGR_ALIAS);

			}
			// gssg - SK��Ʈ�� O2O -- end

			
			
			if ( XLConf.XL_DEBUG_YN ) {
				XLLogger.outputInfoLog("[DEBUG] ----- [JOB] " + this.polName + "/" + this.condWhere + " Query -----");
				
				// gssg - SK��Ʈ�� O2O
				// gssg - linkMode ����
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
	
	
	/// JOB ����
	public void exeJob()
	{
		try {
			
			XLLogger.outputInfoLog(this.logHead + " START JOB : " + this.polName + "/" + this.condWhere);
			
			// 1. Recv Thread ����
			byte sdbType = this.sdbInfo.getDbType();
						
			switch (sdbType) {
				case XLCons.ORACLE:
					oraRecvThread = new XLOracleRecvThread(this);
					oraRecvThread.start();
					break;
					
				// cksohn - xl tibero src ��� �߰�
				case XLCons.TIBERO:
					tiberoRecvThread = new XLTiberoRecvThread(this);
					tiberoRecvThread.start();
					break;
					
				// gssg - xl m2m ���� ���� ����
				// gssg - xl m2m ��� �߰� 
				case XLCons.MYSQL:
					mySQLRecvThread = new XLMySQLRecvThread(this);
					mySQLRecvThread.start();
					break;
				
				// gssg - xl m2m ��� �߰�  - 0413
				case XLCons.MARIADB:
					mariaDBRecvThread = new XLMariaDBRecvThread(this);
					mariaDBRecvThread.start();
					break;
				
				// gssg - xl p2p ����
				// gssg - xl ��ü������ ����2
				case XLCons.PPAS:
					ppasRecvThread = new XLPPASRecvThread(this);
					ppasRecvThread.start();
					break;				
				
				// gssg - xl PPAS/PostgreSQL �и�
				case XLCons.POSTGRESQL:
					postgreSQLRecvThread = new XLPostgreSQLRecvThread(this);
					postgreSQLRecvThread.start();
					break;				
					
				// gssg - ms2ms ����
				case XLCons.MSSQL:
					mssqlRecvThread = new XLMSSQLRecvThread(this);
					mssqlRecvThread.start();
					break;
				
				// gssg - cubrid support
				case XLCons.CUBRID:
					cubridRecvThread = new XLCubridRecvThread(this);
					cubridRecvThread.start();
					break;
					
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Altibase to Altibase ����
				// gssg - Altibase5 to Altibase7 ����
				case XLCons.ALTIBASE:
				case XLCons.ALTIBASE5:				
					altibaseRecvThread = new XLAltibaseRecvThread(this);
					altibaseRecvThread.start();
					break;
					
					
			}
			
			Thread.sleep(500);
			
			// 2. Apply Thread ����
			byte tdbType = this.tdbInfo.getDbType();
			switch (tdbType) {
				case XLCons.ORACLE:
					oraApplyThread = new XLOracleApplyThread(this);
					oraApplyThread.start();
					break;
				
				// gssg - xl t2t ����
				case XLCons.TIBERO:
					tiberoApplyThread = new XLTiberoApplyThread(this);
					tiberoApplyThread.start();
					break;
					
				// cksohn - xl o2p ��� �߰�
				// gssg - xl ��ü������ ����2
				case XLCons.PPAS:
					ppasApplyThread = new XLPPASApplyThread(this);
					ppasApplyThread.start();
					break;
					
				// gssg - xl PPAS/PostgreSQL �и�
				case XLCons.POSTGRESQL:
					postgreSQLApplyThread = new XLPostgreSQLApplyThread(this);
					postgreSQLApplyThread.start();
					break;
					
				// gssg - xl m2m ���� ���� ����
				// gssg - xl m2m ��� �߰�
				case XLCons.MYSQL:
					mySQLApplyThread = new XLMySQLApplyThread(this);
					mySQLApplyThread.start();
					break;
				
				// gssg - xl m2m ��� �߰�  - 0413
				case XLCons.MARIADB:
					mariaDBApplyThread = new XLMariaDBApplyThread(this);
					mariaDBApplyThread.start();
					break;
					
				// gssg - ms2ms ����
				case XLCons.MSSQL:
					mssqlApplyThread = new XLMSSQLApplyThread(this);
					mssqlApplyThread.start();
					break;
					
				// gssg - cubrid support
				case XLCons.CUBRID:
					cubridApplyThread = new XLCubridApplyThread(this);
					cubridApplyThread.start();
					break;
					
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Altibase to Altibase ����
				// gssg - Altibase5 to Altibase7 ����
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
		
			
			// gssg - csv file create ��� �߰�
			if ( XLConf.XL_CREATE_FILE_YN ) {
				XLLogger.outputInfoLog("---create file-----");
				// gssg - csv file create ��� �߰�
				// gssg - csv ���� ����
				this.bulk_pipePath = XLUtil.makeCSV(this);
			} else {				
				// pipe ���� ����
				XLLogger.outputInfoLog("---makePipe-----");
				this.bulk_pipePath = XLUtil.makePipe(this.polName, this.tbSName);
			}
		
			
			// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL - �߰� ���� Ÿ���� ����Ŭ�϶���
			// ���� Ÿ�� DB���� ���� ó���ؾ� �� !!!!!!!!!!!!!!!!!!!!!
			byte tdbType = this.tdbInfo.getDbType();
			if ( tdbType == XLCons.ORACLE ) {
				// ctlfile ����
				this.bulk_ctlFilePath = XLUtil.makeCtlFile_ORACLE(this, this.tbSName);
			
			
				// cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
				this.bulk_logFilePath = this.bulk_ctlFilePath.substring(0, this.bulk_ctlFilePath.lastIndexOf(".")) + ".log";
			
				
				XLLogger.outputInfoLog("bulk_logFilePath-> "+this.bulk_logFilePath);
				
			} else if ( tdbType == XLCons.TIBERO ) {
				// gssg - xl t2t ����
				// gssg - t2t bulk mode ����
				this.bulk_ctlFilePath = XLUtil.makeCtlFile_TIBERO(this, this.tbSName);
				
				this.bulk_logFilePath = this.bulk_ctlFilePath.substring(0, this.bulk_ctlFilePath.lastIndexOf(".")) + ".log";
			}
			
			
			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
			// 2. Apply Thread ����
			// byte tdbType = this.tdbInfo.getDbType();
			switch (tdbType) {
				case XLCons.ORACLE:
					oraApplyBulkThread = new XLOracleApplyBulkThread(this);
					oraApplyBulkThread.start();
					break;
					
				// gssg - xl t2t ����
				case XLCons.TIBERO:					
					tiberoApplyBulkThread = new XLTiberoApplyBulkThread(this);
					tiberoApplyBulkThread.start();
					break;
					
				case XLCons.MARIADB:
					mariaDBApplyBulkThread = new XLMariaDBApplyBulkThread(this);
					mariaDBApplyBulkThread.start();
					break;
					
				// gssg - xl o2m ����
				case XLCons.MYSQL:
					mySQLApplyBulkThread = new XLMySQLApplyBulkThread(this);
					mySQLApplyBulkThread.start();
					break;
					
				// gssg - xl o2p bulk mode ����
				// gssg - ppas apply bulk thread �߰�
				case XLCons.PPAS:
					ppasApplyBulkThread = new XLPPASApplyBulkThread(this);
					ppasApplyBulkThread.start();
					break;
				
				// gssg - xl PPAS/PostgreSQL �и�
				case XLCons.POSTGRESQL:
					postgreSQLApplyBulkThread = new XLPostgreSQLApplyBulkThread(this);
					postgreSQLApplyBulkThread.start();
					break;
					
				// gssg - ms2ms ����
				case XLCons.MSSQL:
					mssqlApplyBulkThread = new XLMSSQLApplyBulkThread(this);
					mssqlApplyBulkThread.start();
					break;
					
			}
			
			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
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
			
			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ���� - comment
			//XLLogger.outputInfoLog("CKSOHN DEBUG SLEEP TEST - !!!");
			// Thread.sleep(3000); //
			
			// 1. Recv Thread ����
			byte sdbType = this.sdbInfo.getDbType();
			
			switch (sdbType) {
			
				case XLCons.ORACLE:
					
					// gssg - �ѱ����� �ҽ� ����Ŭ�� ����
				
					oraRecvBulkThread = new XLOracleRecvBulkThread(this);
					oraRecvBulkThread.start();
										
					
					break;
					
				// cksohn - xl tibero src ��� �߰�
				case XLCons.TIBERO:
					
					// gssg - csv file create ��� �߰�
					// gssg - file create Ŭ���� �и�					
				
					tiberoRecvBulkThread = new XLTiberoRecvBulkThread(this);
					tiberoRecvBulkThread.start();						
					
					
					break;
					
				// gssg - xl m2m bulk mode ����
				case XLCons.MARIADB:
					mariaDBRecvBulkThread = new XLMariaDBRecvBulkThread(this);
					mariaDBRecvBulkThread.start();
					break;
					
				// gssg - xl o2m ����
				case XLCons.MYSQL:
					mySQLRecvBulkThread = new XLMySQLRecvBulkThread(this);
					mySQLRecvBulkThread.start();
					break;
				
				// gssg - xl p2p ����
				case XLCons.PPAS:
					ppasRecvBulkThread = new XLPPASRecvBulkThread(this);
					ppasRecvBulkThread.start();
					break;
				
				// gssg - xl PPAS/PostgreSQL �и�
				case XLCons.POSTGRESQL:
					postgreSQLRecvBulkThread = new XLPostgreSQLRecvBulkThread(this);
					postgreSQLRecvBulkThread.start();
					break;
					
				case XLCons.MSSQL:
					mssqlRecvBulkThread = new XLMSSQLRecvBulkThread(this);
					mssqlRecvBulkThread.start();
					// gssg - LG���� MS2O
					// gssg - ms2o bulk mode ����
					break;
					
					
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Altibase to Oracle ����
				// gssg - Altibase5 to Altibase7 ����
				case XLCons.ALTIBASE:
				case XLCons.ALTIBASE5:
					altibaseRecvBulkThread = new XLAltibaseRecvBulkThread(this);
					altibaseRecvBulkThread.start();
			}

			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ���� - comment
			/***
			// 2. Apply Thread ����
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
	
	// gssg - SK��Ʈ�� O2O
	// gssg - linkMode ����
	public void exeJobLink() {

		try {
			
			XLLogger.outputInfoLog(this.logHead + " START JOB : " + this.polName + "/" + this.condWhere);
			
			// 1. Recv Thread ����
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
	
	// ayzn - XLInit ��� ����  - XLJobRunPol : jobseq �ּ�
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

	
	// cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
	public String getBulk_logFilePath() {
		return bulk_logFilePath;
	}

    // cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó��
	public void setBulk_logFilePath(String bulk_logFilePath) {
		this.bulk_logFilePath = bulk_logFilePath;
	}
	

	public long getWorkPlanSeq() {
		return workPlanSeq;
	}


	public void setWorkPlanSeq(long workPlanSeq) {
		this.workPlanSeq = workPlanSeq;
	}


	// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
	public boolean isRunLoader() {
		return runLoader;
	}

	// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
	public void setRunLoader(boolean runLoader) {
		this.runLoader = runLoader;
	}
	
	// gssg - xl m2m bulk mode logging ����
	// gssg - xl o2p bulk mode ����
	// gssg - ppas bulk mode logging ����
	public void setApplyCnt(long cnt) { // gssg - xl recvCnt�� applyCnt�� ����
		this.applyCnt = cnt;
	}

	// gssg - xl m2m bulk mode logging ����
	// gssg - xl o2p bulk mode ����
	// gssg - ppas bulk mode logging ����
	public long getApplyCnt() { // gssg - xl recvCnt�� applyCnt�� ����
		return applyCnt;
	}

	// gssg - xl ��ü������ ����
    // gssg - m2m bulk mode thread ���� ����
	public boolean isLoadQuery() {
		return loadQuery;
	}

	// gssg - xl ��ü������ ����
    // gssg - m2m bulk mode thread ���� ����
	public void setLoadQuery(boolean loadQuery) {
		this.loadQuery = loadQuery;
	}


	// gssg - xl t2t ����
	// gssg - t2t bulk mode ������ ���� ����
	public boolean isWrite() {
		return writeYN;
	}

	// gssg - xl t2t ����
	// gssg - t2t bulk mode ������ ���� ����
	public void setWrite(boolean writePipe) {
		this.writeYN = writePipe;
	}
	
	// gssg - LG���� MS2O
	// gssg - gssg - �����ڵ� �� ó��
	public String getCustomColname() {
		return customColname;
	}
	public String getCustomValue() {
		return customValue;
	}



	// Thread�� ���� üũ 
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
				
//			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { //cksohn - xl o2p ��� �߰� - ���� �̰���
//				if ( !this.ppasRecvThread.isAlive() ) {
//					return false;
//				} else {
//					return true;
//				}
				
			} else if ( this.sdbInfo.getDbType() == XLCons.TIBERO ) { // cksohn - xl tibero src ��� �߰�

				if ( !this.tiberoRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL) { // gssg - xl m2m ��� �߰�
				if ( !this.mySQLRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.sdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB ����ȭ

				if ( !this.mariaDBRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl p2p ����
				if ( !this.ppasRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL �и�
				if ( !this.postgreSQLRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms ����
				if ( !this.mssqlRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Altibase to Altibase ����
				// gssg - Altibase5 to Altibase7 ����
				if ( !this.altibaseRecvThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.sdbInfo.getDbType() == XLCons.CUBRID ) {
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Cubrid to Cubrid ����
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
				
			} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) { // gssg - xl t2t ����
				if ( !this.tiberoApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.PPAS ) { // cksohn - xl o2p ��� �߰�
				if ( !this.ppasApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL �и�
				if ( !this.postgreSQLApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
				
			} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl m2m ��� �߰�
				if ( !this.mySQLApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB ����ȭ
				if ( !this.mariaDBApplyThread.isAlive() ) {
					return false;
				} else {
					return true;
				}				
			} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms ����
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
			} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl m2m ��� �߰�
				this.mySQLRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB ����ȭ
				this.mariaDBRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl p2p ����
				this.ppasRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL �и�
				this.postgreSQLRecvThread.interrupt();
			} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms ����
				this.mssqlRecvThread.interrupt();
			} 
		} catch (Exception e) {
			
		}
	}
	
	public void stopApplyThread()
	{
		try {
			if ( this.tdbInfo.getDbType() == XLCons.ORACLE ) { // cksohn - xl o2p ��� �߰�
				this.oraApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) { // gssg - xl t2t ����
				this.tiberoApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.PPAS ) { // cksohn - xl o2p ��� �߰�
				this.ppasApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL �и�
				this.postgreSQLApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl m2m ��� �߰�
				this.mySQLApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl MariaDB ����ȭ
				this.mariaDBApplyThread.interrupt();
			} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms ����
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
				// cksohn - XL_BULK_MODE_YN - ���� ����

				// gssg - �ѱ����� �ҽ� ����Ŭ�� ����
				// gssg - oracle fileThread ����
			
				if ( this.oraRecvBulkThread== null || !this.oraRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}					
				

				
			} else if ( this.sdbInfo.getDbType() == XLCons.TIBERO ) { // cksohn - xl tibero src ��� �߰�
				
				// gssg - thread ���� ����
							
				if ( this.tiberoRecvBulkThread== null || !this.tiberoRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
								 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl m2m bulk mode ����
				
				if ( this.mariaDBRecvBulkThread== null || !this.mariaDBRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl o2m ����
				
				if ( this.mySQLRecvBulkThread== null || !this.mySQLRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl p2p ����
				
				if ( this.ppasRecvBulkThread== null || !this.ppasRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL �и�
				
				if ( this.postgreSQLRecvBulkThread== null || !this.postgreSQLRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms ����
				
				if ( this.mssqlRecvBulkThread== null || !this.mssqlRecvBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				} 
				
			} else if ( this.sdbInfo.getDbType() == XLCons.ALTIBASE || this.sdbInfo.getDbType() == XLCons.ALTIBASE5 ) {
				// gssg - ���������ڿ������� �������̰� ���
				// gssg - Altibase to Altibase ����
				// gssg - Altibase5 to Altibase7 ����
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
			} else if ( this.tdbInfo.getDbType() == XLCons.TIBERO ) { // gssg - xl t2t ����
				if ( !this.tiberoApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			}else if ( this.tdbInfo.getDbType() == XLCons.MARIADB ) { // gssg - xl m2m bulk mode ����
				if ( !this.mariaDBApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.MYSQL ) { // gssg - xl o2m ����
				if ( !this.mySQLApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.PPAS ) { // gssg - xl o2p bulk mode ���� 	// gssg - ppas apply bulk thread �߰�
				if ( !this.ppasApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.POSTGRESQL ) { // gssg - xl PPAS/PostgreSQL �и�
				if ( !this.postgreSQLApplyBulkThread.isAlive() ) {
					return false;
				} else {
					return true;
				}
			} else if ( this.tdbInfo.getDbType() == XLCons.MSSQL ) { // gssg - ms2ms ����
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
			// cksohn - xl tibero src ��� �߰� start - [ 
			switch ( this.sdbInfo.getDbType()  ) {
			
				case XLCons.ORACLE:
					this.oraRecvBulkThread.interrupt();
					break;
					
				case XLCons.TIBERO:					
					this.tiberoRecvBulkThread.interrupt();
					break;
				
				// gssg - xl m2m bulk mode ����
				case XLCons.MARIADB:
					this.mariaDBRecvBulkThread.interrupt();
					break;
				
				// gssg - xl o2m ����
				case XLCons.MYSQL:
					this.mySQLRecvBulkThread.interrupt();
					break;
				
				// gssg - xl p2p ����
				case XLCons.PPAS:
					this.ppasRecvBulkThread.interrupt();
					break;

				// gssg - xl PPAS/PostgreSQL �и�
				case XLCons.POSTGRESQL:
					this.postgreSQLRecvBulkThread.interrupt();
					break;
					
				// gssg - ms2ms ����
				case XLCons.MSSQL:
					this.mssqlRecvBulkThread.interrupt();
					break;

			
			}			
			// ] - end cksohn - xl tibero src ��� �߰�

			
		} catch (Exception e) {
			
		}
	}
	
	public void stopApplyBulkThread()
	{
		try {
			
			// this.oraApplyBulkThread.interrupt();
			// cksohn - xl tibero src ��� �߰� - tibero target �� ���� ���ߵ����� �ʾ�����, �ϴ� �̷� ������� ������ ����
			switch ( this.tdbInfo.getDbType() ) {
			
				case XLCons.ORACLE:
					this.oraApplyBulkThread.interrupt();
					break;

				// gssg - xl t2t ����
				case XLCons.TIBERO:
					this.tiberoApplyBulkThread.interrupt();
					break;
					
				// gssg - xl m2m bulk mode ����
				case XLCons.MARIADB:
					this.mariaDBApplyBulkThread.interrupt();
					break;
				
				// gssg - xl o2m ����
				case XLCons.MYSQL:
					this.mySQLApplyBulkThread.interrupt();
					break;
				
				// gssg - xl o2p bulk mode ����
				// gssg - ppas bulk thread �߰�
				case XLCons.PPAS:
					this.ppasApplyBulkThread.interrupt();
					break;
					
				// gssg - xl PPAS/PostgreSQL �и�
				case XLCons.POSTGRESQL:
					this.postgreSQLApplyBulkThread.interrupt();
					break;
				
				// gssg - ms2ms ����
				case XLCons.MSSQL:
					this.mssqlApplyBulkThread.interrupt();
					break;


					
			}			
		} catch (Exception e) {
			
		}
	}




}
