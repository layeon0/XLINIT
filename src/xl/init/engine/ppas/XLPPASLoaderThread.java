package xl.init.engine.ppas;

import java.io.BufferedReader;


import java.io.FileReader;
import java.sql.Statement;
import java.util.Vector;

//gssg - xl PPAS/PostgreSQL �и�
// import org.postgresql.copy.CopyManager;
// import org.postgresql.core.BaseConnection;

// gssg - xl ��ü������ ����2
// gssg - PostgreSQL Ŀ���� ó��
import com.edb.copy.CopyManager;
import com.edb.core.BaseConnection;

import oracle.net.aso.s;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLMySQLConnection;
import xl.init.conn.XLPPASConnection;
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



public class XLPPASLoaderThread extends Thread {
	
	private XLJobRunPol jobRunPol = null;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ����
	private Process pr = null;
	
	// gssg - xl o2p bulk mode ����
	// gssg - mysql -> ppas ����
	private XLPPASConnection ppasConnObj = null;
//	private Statement stmtInsert = null;
	
	public XLPPASLoaderThread(XLJobRunPol jobRunPol) {
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
			
			// gssg - xl o2p bulk mode ����
			// gssg - mysql -> ppas ����
			ppasConnObj = new XLPPASConnection( // gssg - xl m2m ��� �߰�  - 0413
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
			if ( !ppasConnObj.makeConnection() ) {
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
			CopyManager copyManager = new CopyManager((BaseConnection) ppasConnObj.getConnection());
		
			
			// gssg - xl ��ü������ ����
			// gssg - o2p ��ҹ��� ó��
			sb_cmd.append("COPY ");
//				// gssg - SK�ڷ��� O2M, O2P -- start
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
			
			XLLogger.outputInfoLog("[COPY LOAD] Loading to stage " + this.jobRunPol.getTableInfo().getTowner() + "." + 
			this.jobRunPol.getTableInfo().getTtable() + " from csv");

			long loadCnt = copyManager.copyIn(sb_cmd.toString(), br);

			this.jobRunPol.setApplyCnt(loadCnt);
			
        	// gssg - mysql -> ppas ����            
            this.ppasConnObj.commit();

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
			try { if ( this.ppasConnObj != null ) this.ppasConnObj.closeConnection(); } catch (Exception e) {} finally { this.ppasConnObj = null; }

			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();
			
			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
			this.jobRunPol.setRunLoader(false);
						
			}
		
	}
	

}
