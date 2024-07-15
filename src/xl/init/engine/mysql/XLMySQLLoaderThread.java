package xl.init.engine.mysql;

import java.io.BufferedReader;
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
import xl.init.conn.XLMySQLConnection;
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
 * gssg - BULK mode mariadb sqlldr
 *
 */



public class XLMySQLLoaderThread extends Thread {

	
	private XLJobRunPol jobRunPol = null;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	// cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ����
	private Process pr = null;
	
	private XLMySQLConnection mySQLConnObj = null;
	private Statement stmtInsert = null;		
	
	public XLMySQLLoaderThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;

		this.logHead = "[" + this.jobRunPol.getPolName() + "][LOADER]";
	}




	@Override
	public void run(){
	
		// gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ����
		XLJobTableInfo jobTableInfo = this.jobRunPol.getTableInfo();
		
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
			mySQLConnObj = new XLMySQLConnection( // gssg - xl m2m ��� �߰�  - 0413
					tdbInfo.getIp(), 
					tdbInfo.getDbSid(),
					tdbInfo.getUserId(),
					tdbInfo.getPasswd(),
					tdbInfo.getPort(),
					tdbInfo.getDbType() 
					);
			
			// Target DB Connection
			if ( !mySQLConnObj.makeConnection() ) {
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

			this.stmtInsert = (Statement)mySQLConnObj.getConnection().createStatement();
			StringBuffer sb_cmd = new StringBuffer();
			// gssg - xl m2m bulk mode ����
			sb_cmd.append("LOAD DATA LOCAL INFILE '")
			.append(jobRunPol.getBulk_pipePath()) // csv path
			// gssg - xl ��ü������ ����
			// gssg - o2m ��ҹ��� ó��
//			.append("' REPLACE INTO TABLE " + this.jobRunPol.getTableInfo().getTowner() + "." + this.jobRunPol.getTableInfo().getTtable())
			.append("' REPLACE INTO TABLE " + "`" + this.jobRunPol.getTableInfo().getTowner() + "`" + "." + 
					"`" + this.jobRunPol.getTableInfo().getTtable() + "`")
			.append(" CHARACTER SET utf8 ")
			.append("FIELDS TERMINATED BY ',' ") // column delimiter
			.append("ENCLOSED BY '\"' ")
			.append("LINES TERMINATED BY '\\n' ") // line delimiter
			// gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ����
			.append("(");

			// vtData = dataQ.getDataQ();
			
			// �÷� ���� settting
			Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();

			
			 
			// gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ����
			// gssg - xl ��ü������ ����
			// gssg - m2m bulk mode binary Ÿ�� ó��
			HashMap<String, String> binMap = new HashMap<>();			
//			boolean binChk = false;
			
			// gssg - xl function ��� ����
			// gssg - mysql bulk mode function ����
			LinkedHashMap<String, String> funcMap = new LinkedHashMap<String, String>();
			ArrayList<String> funcArrList = new ArrayList<String>();
			boolean setChk = false;
			int cnt = 0;

			for(int i = 0; i < vtJobColInfo.size();i++) {					
					XLJobColInfo colInfo = vtJobColInfo.get(i);					
					if(		
				            // gssg - īī�� - m2m bulk mode ����
							colInfo.getDataType() == XLDicInfoCons.BIT || 
							colInfo.getDataType() == XLDicInfoCons.BINARY || 
							colInfo.getDataType() == XLDicInfoCons.VARBINARY || 
							colInfo.getDataType() == XLDicInfoCons.TINYBLOB ) {
						binMap.put("`" + colInfo.getColName_map() + "`", "@var"+i);
						sb_cmd.append("@var" + i);
						setChk = true;
					} else {
						// gssg - xl function ��� ����
						// gssg - mysql bulk mode function ����						
						if( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
							funcMap.put("`" + colInfo.getColName_map() + "`", "@var"+i);
							sb_cmd.append("@var" + i);
							funcArrList.add(cnt++, colInfo.getFunctionStr());
							setChk = true;
						} else {
							sb_cmd.append("`" + colInfo.getColName_map() + "`");							
						}						
					}
					
					if(i != vtJobColInfo.size() -1)
						sb_cmd.append(", ");
			 }
			 if(setChk) {
					sb_cmd.append(") SET ");				 
			 } else {
				 sb_cmd.append(")");
			 }


			// gssg - xl function ��� ����
			// gssg - mysql bulk mode function ����
			cnt = 0;			
			for(java.util.Map.Entry<String, String> entry : funcMap.entrySet()) {				
				if(cnt != 0) {
					sb_cmd.append(", ");
				}
				sb_cmd.append(entry.getKey() + "=" + funcArrList.get(cnt) + "(" + entry.getValue() + ")");
				cnt++;
			}
			
			for(java.util.Map.Entry<String, String> entry : binMap.entrySet()) {				
				if(cnt != 0) {
					sb_cmd.append(", ");
				}
				sb_cmd.append(entry.getKey() + "=" + "UNHEX(" + entry.getValue() + ")");
				cnt++;
			}
			sb_cmd.append(";");

			// gssg - xl ��ü������ ����
            // gssg - m2m bulk mode thread ���� ����
			this.jobRunPol.setRunLoader(true);

			stmtInsert.executeQuery(sb_cmd.toString());
			
            this.mySQLConnObj.commit();
            
			// gssg - xl ��ü������ ����
            // gssg - m2m bulk mode thread ���� ����
            // gssg - īī�� - m2m bulk mode ����
////        this.jobRunPol.setLoadQuery(true);           
            
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
			try { if ( this.mySQLConnObj != null ) this.mySQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.mySQLConnObj = null; }

			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();

			// cksohn - XL_BULK_MODE_YN - sqlldr ������� ����
			this.jobRunPol.setRunLoader(false);

			}
		
		// }
		
	}
	

}
