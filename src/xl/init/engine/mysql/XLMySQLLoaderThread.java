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
	
	// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정
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
	
		// gssg - xl 타겟이 MySQL일 경우 bulk mode 컬럼 매핑 지원
		XLJobTableInfo jobTableInfo = this.jobRunPol.getTableInfo();
		
		// gssg - xl m2m bulk mode 지원
		// synchronized(this) {
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting Loader Thread..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
		
			// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - comment
			// Process pr = null;				
					

			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			// gssg - xl m2m bulk mode 지원
			mySQLConnObj = new XLMySQLConnection( // gssg - xl m2m 기능 추가  - 0413
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

			this.stmtInsert = (Statement)mySQLConnObj.getConnection().createStatement();
			StringBuffer sb_cmd = new StringBuffer();
			// gssg - xl m2m bulk mode 지원
			sb_cmd.append("LOAD DATA LOCAL INFILE '")
			.append(jobRunPol.getBulk_pipePath()) // csv path
			// gssg - xl 전체적으로 보완
			// gssg - o2m 대소문자 처리
//			.append("' REPLACE INTO TABLE " + this.jobRunPol.getTableInfo().getTowner() + "." + this.jobRunPol.getTableInfo().getTtable())
			.append("' REPLACE INTO TABLE " + "`" + this.jobRunPol.getTableInfo().getTowner() + "`" + "." + 
					"`" + this.jobRunPol.getTableInfo().getTtable() + "`")
			.append(" CHARACTER SET utf8 ")
			.append("FIELDS TERMINATED BY ',' ") // column delimiter
			.append("ENCLOSED BY '\"' ")
			.append("LINES TERMINATED BY '\\n' ") // line delimiter
			// gssg - xl 타겟이 MySQL일 경우 bulk mode 컬럼 매핑 지원
			.append("(");

			// vtData = dataQ.getDataQ();
			
			// 컬럼 정보 settting
			Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();

			
			 
			// gssg - xl 타겟이 MySQL일 경우 bulk mode 컬럼 매핑 지원
			// gssg - xl 전체적으로 보완
			// gssg - m2m bulk mode binary 타입 처리
			HashMap<String, String> binMap = new HashMap<>();			
//			boolean binChk = false;
			
			// gssg - xl function 기능 지원
			// gssg - mysql bulk mode function 지원
			LinkedHashMap<String, String> funcMap = new LinkedHashMap<String, String>();
			ArrayList<String> funcArrList = new ArrayList<String>();
			boolean setChk = false;
			int cnt = 0;

			for(int i = 0; i < vtJobColInfo.size();i++) {					
					XLJobColInfo colInfo = vtJobColInfo.get(i);					
					if(		
				            // gssg - 카카오 - m2m bulk mode 보완
							colInfo.getDataType() == XLDicInfoCons.BIT || 
							colInfo.getDataType() == XLDicInfoCons.BINARY || 
							colInfo.getDataType() == XLDicInfoCons.VARBINARY || 
							colInfo.getDataType() == XLDicInfoCons.TINYBLOB ) {
						binMap.put("`" + colInfo.getColName_map() + "`", "@var"+i);
						sb_cmd.append("@var" + i);
						setChk = true;
					} else {
						// gssg - xl function 기능 지원
						// gssg - mysql bulk mode function 지원						
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


			// gssg - xl function 기능 지원
			// gssg - mysql bulk mode function 지원
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

			// gssg - xl 전체적으로 보완
            // gssg - m2m bulk mode thread 순서 조정
			this.jobRunPol.setRunLoader(true);

			stmtInsert.executeQuery(sb_cmd.toString());
			
            this.mySQLConnObj.commit();
            
			// gssg - xl 전체적으로 보완
            // gssg - m2m bulk mode thread 순서 조정
            // gssg - 카카오 - m2m bulk mode 보완
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

			// gssg - xl m2m bulk mode 지원
			try { if ( this.stmtInsert != null ) this.stmtInsert.close(); } catch (Exception e) {} finally { this.stmtInsert = null; }
			try { if ( this.mySQLConnObj != null ) this.mySQLConnObj.closeConnection(); } catch (Exception e) {} finally { this.mySQLConnObj = null; }

			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();

			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			this.jobRunPol.setRunLoader(false);

			}
		
		// }
		
	}
	

}
