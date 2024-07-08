package xl.init.engine.oracle;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;



/**
 * 
 * @author cksohn
 * 
 * cksohn - BULK mode oracle sqlldr
 *
 */

public class XLOracleLoaderThread extends Thread {

	
	
	private XLJobRunPol jobRunPol = null;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정
	private Process pr = null;
	
	public XLOracleLoaderThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][LOADER]";
	}


	@Override
	public void run(){
		
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간
		
		/**
		 * [예제]
		 *  sqlldr xladmin/xladmin@192.168.0.31:1522/orcl11g control='/datainfo/xladmin/ins_xl_test1234.ctl' readsize=20000000 bindsize=20000000 rows=5000

			# direct=true
			sqlldr xladmin/xladmin@192.168.0.31:1522/orcl11g control='/datainfo/xladmin/ins_xl_test1234.ctl' direct=true
		 */
		try {
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting Loader Thread(Direct Path Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
		
			// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발생 오류 수정 - comment
			// Process pr = null;				
					

			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			StringBuffer sb_cmd = new StringBuffer();
			sb_cmd.append("sqlldr")
			.append(" " + tdbInfo.getUserId()) // user id
			.append("/" + tdbInfo.getPasswd()) // user passwd
			.append("@" + tdbInfo.getIp()) // target ip
			.append(":" + tdbInfo.getPort()) // target port
			.append("/" + tdbInfo.getDbServiceName()) // target sid
			.append(" control='" + this.jobRunPol.getBulk_ctlFilePath() + "'") // ctl file path
			.append(" log='" + this.jobRunPol.getBulk_logFilePath() + "'") // log file path
			.append(" direct=true"); // sqlldr option
			
	
			
			// if ( XLConf.XL_MGR_DEBUG_YN ) {
				XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][LOADER CMD] " + sb_cmd.toString());
			// }
 
			pr = Runtime.getRuntime().exec( sb_cmd.toString() ); 
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			this.jobRunPol.setRunLoader(true);
			
			
			// 여기로 위치 이동
			BufferedReader reader = null;
			try {
				
				reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));			
				
				//StringBuffer vwResultInfo = new StringBuffer();
				String sLine = "";
				while ( (sLine = reader.readLine()) != null ) {
					
					// gssg - o2o damo 적용
					// gssg - bulk mode thread 순서 조정
//					System.out.println("#####################################################");						
//					System.out.println(sLine);
//					System.out.println("#####################################################");
//					vwResultInfo.append(sLine + "\n");
					
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - sql loader commit 기능 추가
//					XLLogger.outputInfoLog(sLine);
					
				}
				
				// XLLogger.outputInfoLog("CKSOHN DEBUG::: SQLLDR RESULT = " + vwResultInfo.toString());
			} catch (Exception ee) {
				ee.printStackTrace();
				
			} finally {
				try { if ( reader != null ) reader.close(); } catch (Exception e) {} finally { reader = null;}
			}
			
			pr.waitFor(); 
			
			
			
//			BufferedReader reader = null;
//			try {
//				
//				reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));			
//				
//				//StringBuffer vwResultInfo = new StringBuffer();
//				String sLine = "";
//				while ( (sLine = reader.readLine()) != null ) {
//					/**
//					 *  SQL*Loader-704: Internal error: ulconnect: OCIServerAttach [0]
//						ORA-12514: TNS:listener does not currently know of service requested in connect descriptor
//					 */
//					// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
//					// 접속실패시 실패시 처리 - 접속실패시 대략 위와 같은   에러가 발생함.
//					if ( sLine.contains("error") || sLine.contains("ORA-")) {
//						XLLogger.outputInfoLog("[EXCEPTION] Failed to call loader. - " + sLine);
//					}
//					
//					//vwResultInfo.append(sLine + "\n");
//				}
//				
//				// XLLogger.outputInfoLog("CKSOHN DEBUG::: SQLLDR RESULT = " + vwResultInfo.toString());
//			} catch (Exception ee) {
//				ee.printStackTrace();
//				
//			} finally {
//				try { if ( reader != null ) reader.close(); } catch (Exception e) {} finally { reader = null;}
//			}
			
			XLLogger.outputInfoLog(this.logHead + " Finished ApplyLoaderThread(BULK MODE)");
			
		} catch (InterruptedException ie) { // cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - 추가 처리
			
			XLLogger.outputInfoLog("[WARN] Loader is Interrupted." );
			
			this.jobRunPol.setStopJobFlag(true);
			
			// errMsg = ie.toString();
			errMsg = "Loader Thread(Direct Path mode) is stopped by interruption.";
			
			XLLogger.outputInfoLog(this.logHead + "Loader Thread(Direct Path mode) is stopped by interruption.");
			
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.setErrMsg_Loader(this.logHead +"[EXCEPTION] " +  errMsg);
			

			
		} catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Loader Thread(Direct Path mode) is stopped abnormal.");
			
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.setErrMsg_Loader(this.logHead +"[EXCEPTION] " +  errMsg);
			
			
			
			
		} finally {
			
			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.getDataQ().notifyEvent();
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			this.jobRunPol.setRunLoader(false);
			
			// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정
			try { pr.destroy(); } catch (Exception ee) {}

		}
		
	}
	

}
