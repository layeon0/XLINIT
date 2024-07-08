package xl.init.engine.mssql;

import java.io.BufferedReader;
import java.io.File;
import java.security.KeyStore.Entry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Vector;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.lib.common.XLCons;
import xl.init.conn.XLMSSQLConnection;
import xl.init.conn.XLMariaDBConnection;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDataQ;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLJobTableInfo;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;



/**
 * 
 * @author gssg
 * 
 * gssg - BULK mode mssql sqlldr
 *
 */

// gssg - ms2ms ����


public class XLMSSQLLoaderThread extends Thread {

	
	private XLJobRunPol jobRunPol = null;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ����
	private Process pr = null;
	
	private XLMSSQLConnection mssqlConnObj = null;
	private Statement stmtInsert = null;		
	
	public XLMSSQLLoaderThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;

		this.logHead = "[" + this.jobRunPol.getPolName() + "][LOADER]";
	}




	@Override
	public void run(){
	
		// gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ����
//		XLJobTableInfo jobTableInfo = this.jobRunPol.getTableInfo();
		
		// gssg - xl m2m bulk mode ����
		// synchronized(this) {
		
		long stime = 0; // ���� �ð�
		long etime = 0; // ���� �ð�
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting Loader Thread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
		
			// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� - comment
			// Process pr = null;				
					

			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			// gssg - xl m2m bulk mode ����
			// gssg - ms2ms ����
			// gssg - IDENTITY �÷� ó��
			mssqlConnObj = new XLMSSQLConnection( // gssg - xl m2m ��� �߰�  - 0413
					tdbInfo.getIp(), 
					tdbInfo.getDbSid(),
					tdbInfo.getUserId(),
					tdbInfo.getPasswd(),
					tdbInfo.getPort(),
					tdbInfo.getDbType(),
					false
					);
			
			// Target DB Connection
			if ( !mssqlConnObj.makeConnection() ) {
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

			this.stmtInsert = (Statement)mssqlConnObj.getConnection().createStatement();
			StringBuffer sb_cmd = new StringBuffer();

			// gssg - ms2ms ����
			// gssg - bulk insert ����
			File pipeFile = new File(this.jobRunPol.getBulk_pipePath());

			sb_cmd.append("BULK INSERT ")
			.append(this.jobRunPol.getTableInfo().getTowner() + "." + this.jobRunPol.getTableInfo().getTtable())
			.append(" FROM '\\\\" + this.jobRunPol.getTdbInfo().getIp() + "\\" + "xl_pipe\\" + pipeFile.getName()) // csv path
//			.append(" FROM '\\\\" + this.jobRunPol.getTdbInfo().getIp() + "\\" + "xl_pipe\\" + "ms_test01.csv") // csv path
			.append( "' WITH ")
			.append("(FORMAT = 'CSV'")
			.append(", FIELDQUOTE = '\"'")
			.append(", FIELDTERMINATOR = ','")
			.append(", ROWTERMINATOR = '0x0a')");
			
			System.out.println(sb_cmd.toString());

			// gssg - xl ��ü������ ����
            // gssg - m2m bulk mode thread ���� ����
			this.jobRunPol.setRunLoader(true);

				
			stmtInsert.execute(sb_cmd.toString());
			
            this.mssqlConnObj.commit();
            
			// gssg - xl ��ü������ ����
            // gssg - m2m bulk mode thread ���� ����
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

			// gssg - xl m2m bulk mode ����
			try { if ( this.stmtInsert != null ) this.stmtInsert.close(); } catch (Exception e) {} finally { this.stmtInsert = null; }
			try { if ( this.mssqlConnObj != null ) this.mssqlConnObj.closeConnection(); } catch (Exception e) {} finally { this.mssqlConnObj = null; }

			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();

			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
			this.jobRunPol.setRunLoader(false);

			}
		
		// }
		
	}
	

}
