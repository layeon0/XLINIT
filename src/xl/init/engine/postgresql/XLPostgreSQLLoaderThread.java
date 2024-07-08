package xl.init.engine.postgresql;

import java.io.BufferedReader;


import java.io.FileReader;
import java.io.StringReader;
import java.sql.Statement;
import java.util.Vector;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

// gssg - xl ��ü������ ����2
// gssg - PostgreSQL Ŀ���� ó��
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
	
	// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ����
	private Process pr = null;
	
	private long totalCommitCnt =0;
	
	// gssg - t2p ����
	// gssg - postgreSQL Ŀ���� ����
//	private XLPPASConnection ppasConnObj = null;
	private XLPostgreSQLConnection postgreSQLConnObj = null;
	
	
	public XLPostgreSQLLoaderThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;

		// gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ����
		// this.vtColInfo = this.jobRunPol.getTableInfo().getVtColInfo();
		// this.dataQ = this.jobRunPol.getDataQ();
		this.logHead = "[" + this.jobRunPol.getPolName() + "][LOADER]";
	}


	@Override
	public void run(){
	
		// gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ����
		XLJobTableInfo jobTableInfo = this.jobRunPol.getTableInfo();
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
		BufferedReader br = null;
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting Loader Thread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");

			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();			
			
			// gssg - t2p ����
			// gssg - postgreSQL Ŀ���� ����
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
			// gssg - xl o2p bulk mode ����
			// gssg - mysql -> ppas ����
			if ( !postgreSQLConnObj.makeConnection() ) {
				errMsg = "[EXCEPTION] Apply : Failed to make target db connection - " + tdbInfo.getIp() + "/" + tdbInfo.getDbSid();
				XLLogger.outputInfoLog(this.logHead + errMsg);
				// TODO ���⼭ Catalog DB�� ���з� update ġ�� ������ �ϴµ�,, catalog �� Ÿ�ٿ� ���� ��� ������ �Ǳ���. 
				//      �׷���, ���� ����� �����ϰ� JOBQ�� ����� �����ϵ��� ��ġ�ؾ� �� ���� ����. 
				
				this.jobRunPol.setStopJobFlag(true);
				XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Apply Thread is stopped abnormal.");
				
				this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
				this.jobRunPol.setErrMsg_Apply(errMsg);
				return;				
			} else {
				XLLogger.outputInfoLog(this.logHead + " Target DBMS is connected - " +  tdbInfo.getIp() + "/" + tdbInfo.getDbSid());
			}

			
			// gssg - xl o2p bulk mode ����
			// gssg - mysql -> ppas ����			
//			this.stmtInsert = (Statement)ppasConnObj.getConnection().createStatement();

			// gssg - ppas bulk mode thread ���� ����
			this.jobRunPol.setRunLoader(true);

			br = new BufferedReader(new FileReader(this.jobRunPol.getBulk_pipePath()), 1024 * 1000 * 1);

			
			StringBuffer sb_cmd = new StringBuffer();
			
			// gssg - ppas copymgr ����
			// gssg - xl ��ü������ ����2
			// gssg - PostgreSQL Ŀ���� ó��
			CopyManager copyManager = new CopyManager((BaseConnection) postgreSQLConnObj.getConnection());

			// gssg - xl ��ü������ ����
			// gssg - o2p ��ҹ��� ó��
			sb_cmd.append("COPY ");
			
			// gssg - SK�ڷ��� O2M, O2P -- start
			// .append( "\"" + this.jobRunPol.getTableInfo().getTowner() + "\"" + "." + "\"" + this.jobRunPol.getTableInfo().getTtable() + "\"" )
			if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL || this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS ) {
				sb_cmd.append( "\"" + this.jobRunPol.getTableInfo().getTowner() + "\"" + "." + "\"" + this.jobRunPol.getTableInfo().getTtable() + "\"" );
			} else {				
				sb_cmd.append(this.jobRunPol.getTableInfo().getTowner() + "." + this.jobRunPol.getTableInfo().getTtable());
			}
			
			
			// gssg - SK�ڷ��� O2M, O2P -- end
			
			sb_cmd.append("(");
			
			// �÷� ���� settting
			Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();

			for(int i = 0; i< vtJobColInfo.size();i++) {					
				XLJobColInfo colInfo = vtJobColInfo.get(i);
				
				// gssg - SK�ڷ��� O2M, O2P -- start
				// sb_cmd.append( "\"" + colInfo.getColName_map() + "\"" );
				if ( this.jobRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL || this.jobRunPol.getSdbInfo().getDbType() == XLCons.PPAS ) {
					sb_cmd.append( "\"" + colInfo.getColName_map() + "\"" );
				} else {
					sb_cmd.append(colInfo.getColName_map());
				}
				// gssg - SK�ڷ��� O2M, O2P -- end
			
				if(i != vtJobColInfo.size() -1)
					sb_cmd.append(", ");
			 }
			
			sb_cmd.append(") ")
			.append("FROM STDIN WITH CSV DELIMITER ',' QUOTE '\"' ESCAPE '\\' NULL AS '\\N' ENCODING 'UTF8'");
			
			//XLLogger.outputInfoLog(sb_cmd.toString());
			XLLogger.outputInfoLog("[COPY LOAD] Loading to stage " + this.jobRunPol.getTableInfo().getTowner() + "." + 
			this.jobRunPol.getTableInfo().getTtable() + " from csv");

			//10���Ǿ� commit
			/*
			 * int currentBatchSize = 0;
			 * 
			 * 
			 * String line; long BATCH_SIZE = 100; StringBuffer sb = new StringBuffer();
			 * 
			 * while ((line = br.readLine()) != null) { sb.append(line).append("\n");
			 * currentBatchSize++;
			 * 
			 * // BATCH_SIZE�� �����ϸ� Ŀ�� if (currentBatchSize >= BATCH_SIZE) { StringReader sr
			 * = new StringReader(sb.toString()); copyManager.copyIn(sb_cmd.toString(), sr);
			 * 
			 * this.postgreSQLConnObj.commit(); XLLogger.outputInfoLog(this.logHead +
			 * " Apply Count : " + currentBatchSize);
			 * 
			 * this.totalCommitCnt += currentBatchSize; if ( XLConf.XL_DEBUG_YN ) {
			 * XLLogger.outputInfoLog("[DEBUG] commitCnt-1 : " + this.totalCommitCnt); }
			 * 
			 * currentBatchSize = 0; // ��ġ ũ�� �ʱ�ȭ sb.setLength(0); // StringBuilder �ʱ�ȭ } }
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
			
        	// gssg - mysql -> ppas ����            
            this.postgreSQLConnObj.commit();

            // gssg - xl ��ü������ ����
            // gssg - o2p bulk mode thread ���� ����
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
			// gssg - xl o2p bulk mode ����
			// gssg - mysql -> ppas ����
			try { if ( this.postgreSQLConnObj != null ) this.postgreSQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.postgreSQLConnObj = null; }

			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();
			
			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
			this.jobRunPol.setRunLoader(false);
						
			}
		
	}
	

}
