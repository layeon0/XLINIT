package xl.init.engine.postgresql;

import java.io.BufferedReader;


import java.io.FileReader;
import java.io.StringReader;
import java.sql.Statement;
import java.util.Vector;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

// gssg - xl 전체적으로 보완2
// gssg - PostgreSQL 커넥터 처리
// import com.edb.copy.CopyManager;
// import com.edb.core.BaseConnection;

import oracle.net.aso.s;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conf.XLConf;
import xl.init.conn.XLMySQLConnection;
import xl.init.conn.XLPPASConnection;
import xl.init.conn.XLPostgreSQLConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLJobTableInfo;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
//import xl.init.poll.XLPollingEventQ;
import xl.init.util.XLException;
import xl.init.util.XLUtil;



/**
 * 
 * @author gssg
 * 
 * gssg - BULK mode ppas sqlldr
 *
 */



public class XLPostgreSQLLoaderThread extends Thread {
	
	private XLJobRunPol jobRunPol = null;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정
	private Process pr = null;
	
	private long totalCommitCnt =0;
	
	// gssg - t2p 보완
	// gssg - postgreSQL 커넥터 수정
//	private XLPPASConnection ppasConnObj = null;
	private XLPostgreSQLConnection postgreSQLConnObj = null;
	
	
	public XLPostgreSQLLoaderThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;

		// gssg - xl 타겟이 MySQL일 경우 bulk mode 컬럼 매핑 지원
		// this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		// this.dataQ = this.jobRunPol.getDataQ();
		this.logHead = "[" + this.jobRunPol.getPolName() + "][LOADER]";
	}


	@Override
	public void run(){
	
		// gssg - xl 타겟이 MySQL일 경우 bulk mode 컬럼 매핑 지원
		XLJobTableInfo jobTableInfo = this.jobRunPol.getTableInfo();
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		BufferedReader br = null;
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting Loader Thread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");

			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();			
			
			// gssg - t2p 보완
			// gssg - postgreSQL 커넥터 수정
//			ppasConnObj = new XLPPASConnection(
//					tdbInfo.getIp(), 
//					tdbInfo.getDbSid(),
//					tdbInfo.getUserId(),
//					tdbInfo.getPasswd(),
//					tdbInfo.getPort(),
//					tdbInfo.getDbType() 
//					);
			postgreSQLConnObj = new XLPostgreSQLConnection(
			tdbInfo.getIp(), 
			tdbInfo.getDbSid(),
			tdbInfo.getUserId(),
			tdbInfo.getPasswd(),
			tdbInfo.getPort(),
			tdbInfo.getDbType() 
			);
			
			
			// Target DB Connection
			// gssg - xl o2p bulk mode 지원
			// gssg - mysql -> ppas 변경
			if ( !postgreSQLConnObj.makeConnection() ) {
				errMsg = "[EXCEPTION] Apply : Failed to make target db connection - " + tdbInfo.getIp() + "/" + tdbInfo.getDbSid();
				XLLogger.outputInfoLog(this.logHead + errMsg);
				// TODO 여기서 Catalog DB에 실패로 update 치고 끝나야 하는데,, catalog 가 타겟에 있을 경우 문제가 되긴함. 
				//      그러면, 추후 수행시 깨끗하게 JOBQ를 지우고 수행하도록 조치해야 할 수도 있음. 
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Apply(errMsg);
				return;				
			} else {
				XLLogger.outputInfoLog(this.logHead + " Target DBMS is connected - " +  tdbInfo.getIp() + "/" + tdbInfo.getDbSid());
			}

			
			// gssg - xl o2p bulk mode 지원
			// gssg - mysql -> ppas 변경			
//			this.stmtInsert = (Statement)ppasConnObj.getConnection().createStatement();

			// gssg - ppas bulk mode thread 순서 조정
			this.jobRunPol.setRunLoader(true);

			br = new BufferedReader(new FileReader(this.jobRunPol.getBulk_pipePath()), 1024 * 1000 * 1);

			
			StringBuffer sb_cmd = new StringBuffer();
			
			// gssg - ppas copymgr 적용
			// gssg - xl 전체적으로 보완2
			// gssg - PostgreSQL 커넥터 처리
			CopyManager copyManager = new CopyManager((BaseConnection) postgreSQLConnObj.getConnection());

			// gssg - xl 전체적으로 보완
			// gssg - o2p 대소문자 처리
			sb_cmd.append("COPY ");
			
			// gssg - SK텔레콤 O2M, O2P -- start
			// .append( "\"" + this.jobRunPol.getTableInfo().getTowner() + "\"" + "." + "\"" + this.jobRunPol.getTableInfo().getTtable() + "\"" )
			if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL || this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS ) {
				sb_cmd.append( "\"" + this.jobRunPol.getTableInfo().getTowner() + "\"" + "." + "\"" + this.jobRunPol.getTableInfo().getTtable() + "\"" );
			} else {				
				sb_cmd.append(this.jobRunPol.getTableInfo().getTowner() + "." + this.jobRunPol.getTableInfo().getTtable());
			}
			
			
			// gssg - SK텔레콤 O2M, O2P -- end
			
			sb_cmd.append("(");
			
			// 컬럼 정보 settting
			Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();

			for(int i = 0; i< vtJobColInfo.size();i++) {					
				XLJobColInfo colInfo = vtJobColInfo.get(i);
				
				// gssg - SK텔레콤 O2M, O2P -- start
				// sb_cmd.append( "\"" + colInfo.getColName_map() + "\"" );
				if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL || this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS ) {
					sb_cmd.append( "\"" + colInfo.getColName_map() + "\"" );
				} else {
					sb_cmd.append(colInfo.getColName_map());
				}
				// gssg - SK텔레콤 O2M, O2P -- end
			
				if(i != vtJobColInfo.size() -1)
					sb_cmd.append(", ");
			 }
			
			sb_cmd.append(") ")
			.append("FROM STDIN WITH CSV DELIMITER ',' QUOTE '\"' ESCAPE '\\' NULL AS '\\N' ENCODING 'UTF8'");
			
			//XLLogger.outputInfoLog(sb_cmd.toString());
			XLLogger.outputInfoLog("[COPY LOAD] Loading to stage " + this.jobRunPol.getTableInfo().getTowner() + "." + 
			this.jobRunPol.getTableInfo().getTtable() + " from csv");

			//10만건씩 commit
			/*
			 * int currentBatchSize = 0;
			 * 
			 * 
			 * String line; long BATCH_SIZE = 100; StringBuffer sb = new StringBuffer();
			 * 
			 * while ((line = br.readLine()) != null) { sb.append(line).append("\n");
			 * currentBatchSize++;
			 * 
			 * // BATCH_SIZE에 도달하면 커밋 if (currentBatchSize >= BATCH_SIZE) { StringReader sr
			 * = new StringReader(sb.toString()); copyManager.copyIn(sb_cmd.toString(), sr);
			 * 
			 * this.postgreSQLConnObj.commit(); XLLogger.outputInfoLog(this.logHead +
			 * " Apply Count : " + currentBatchSize);
			 * 
			 * this.totalCommitCnt += currentBatchSize; if ( XLConf.XL_DEBUG_YN ) {
			 * XLLogger.outputInfoLog("[DEBUG] commitCnt-1 : " + this.totalCommitCnt); }
			 * 
			 * currentBatchSize = 0; // 배치 크기 초기화 sb.setLength(0); // StringBuilder 초기화 } }
			 * 
			 * if (currentBatchSize > 0) {
			 * 
			 * StringReader sr = new StringReader(sb.toString());
			 * copyManager.copyIn(sb_cmd.toString(), sr); this.postgreSQLConnObj.commit();
			 * XLLogger.outputInfoLog(this.logHead + " Apply Count : " + currentBatchSize);
			 * }
			 */
            
        
			long loadCnt = copyManager.copyIn(sb_cmd.toString(), br);
			
			this.jobRunPol.setApplyCnt(loadCnt);
			
        	// gssg - mysql -> ppas 변경            
            this.postgreSQLConnObj.commit();

            // gssg - xl 전체적으로 보완
            // gssg - o2p bulk mode thread 순서 조정
            this.jobRunPol.setLoadQuery(true);
			
			// if ( XLConf.XL_MGR_DEBUG_YN ) {
			XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][LOADER CMD] " + sb_cmd.toString());
			// }
			
			XLLogger.outputInfoLog(this.logHead + " Finished ApplyLoaderThread(BULK MODE)");
			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Loader Thread(Direct Path mode) is stopped abnormal.");
			
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.setErrMsg_Loader(this.logHead +"[EXCEPTION] " +  errMsg);
			
			
			
			
		} finally {

//			try { if ( this.stmtInsert != null ) this.stmtInsert.close(); } catch (Exception e) {} finally { this.stmtInsert = null; }
			// gssg - xl o2p bulk mode 지원
			// gssg - mysql -> ppas 변경
			try { if ( this.postgreSQLConnObj != null ) this.postgreSQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.postgreSQLConnObj = null; }

			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			this.jobRunPol.setRunLoader(false);
						
			}
		
	}
	

}
