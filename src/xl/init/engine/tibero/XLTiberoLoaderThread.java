package xl.init.engine.tibero;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLJobRunPol;
import xl.init.main.XLOGCons;
import xl.init.main.XLInit;
import xl.init.util.XLException;



/**
 * 
 * @author gssg
 * 
 * gssg - BULK mode tibero tbloader
 *
 */

public class XLTiberoLoaderThread extends Thread {

	private XLJobRunPol jobRunPol = null;
	
	private String errMsg = null;
	
	private String logHead = "";
	
	// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발생 오류 수정
	private Process pr = null;
	
	// gssg - xl t2t 지원
	private List<String> cmd = new ArrayList<String>();
	private ProcessBuilder pb = null;

	
	public XLTiberoLoaderThread(XLJobRunPol jobRunPol) {
		super();
		this.jobRunPol = jobRunPol;
		
		
		this.logHead = "[" + this.jobRunPol.getPolName() + "][LOADER]";
	}


	@Override
	public void run(){
		
		
		long stime = 0; // 시작 시간
		long etime = 0; // 종료 시간

		
		try {

		
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog(this.logHead + " Starting Loader Thread(Direct Path Mode)..." + this.jobRunPol.getCondWhere());
			XLLogger.outputInfoLog("");
			
		
			// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발생 오류 수정 - comment
			// Process pr = null;				
					

			XLDBMSInfo tdbInfo = this.jobRunPol.getTdbInfo();
			
			StringBuffer sb_cmd = new StringBuffer();
			// gssg - xl t2t 지원
			// gssg - t2t bulk mode 지원
			sb_cmd.append("tbloader")
			.append(" userid=" + tdbInfo.getUserId()) // user id
			.append("/" + tdbInfo.getPasswd()) // user passwd
			.append("@" + tdbInfo.getDbServiceName()) // target sid			
			.append(" control='" + this.jobRunPol.getBulk_ctlFilePath() + "'") // ctl file path
			
			// cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리
			.append(" log='" + this.jobRunPol.getBulk_logFilePath() + "'") // log file path

			// gssg - 현대차 대방동 - tbloadr 구문 수정
			.append(" direct=Y"); // sqlldr option
//			.append(" direct=true"); // sqlldr option
//			.append(" rows=" + this.jobRunPol.getPolCommitCnt());


//			XLLogger.outputInfoLog("###################################################################################");
//			XLLogger.outputInfoLog("CKSOHN DEBUG################## IMSI SID --> SERVICE NAME HARD CODING !!!!!!! ##########");
//			XLLogger.outputInfoLog("###################################################################################");

			
			// if ( XLConf.XL_MGR_DEBUG_YN ) {
				XLLogger.outputInfoLog("[" + this.jobRunPol.getPolName() + "][LOADER CMD] " + sb_cmd.toString());
			// }
 

			// gssg - xl t2t 지원
//			cmd.add("/bin/bash");
//			cmd.add("-c");
//			cmd.add(sb_cmd.toString());
//
//			pb = new ProcessBuilder(cmd);
//			pb.redirectErrorStream(true);
//				
//			pr = pb.start();

//				XLLogger.outputInfoLog("@@@@@@@@@@@@@@@@@@[GSSG DEBUG] THIS POINT IS AFTER pr = pb.start();@@@@@@@@@@@@@@@@@@@");

			this.jobRunPol.setRunLoader(true);

//				XLLogger.outputInfoLog("@@@@@@@@@@@@@@@@@@[GSSG DEBUG] THIS POINT IS AFTER this.jobRunPol.setRunLoader(true);@@@@@@@@@@@@@@@@@@@");

				

			pr = Runtime.getRuntime().exec( sb_cmd.toString() );					

			
				// 여기로 위치 이동
			BufferedReader reader = null;
			try {
					
					reader = new BufferedReader(new InputStreamReader(pr.getInputStream()));			
					
					//StringBuffer vwResultInfo = new StringBuffer();
					String sLine = "";
					while ( (sLine = reader.readLine()) != null ) {
						
//						System.out.println("#####################################################");						
//						System.out.println(sLine);
//						System.out.println("#####################################################");
						
						
//						System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");						
//						System.out.println("[GSSG DEBUG]" + this.jobRunPol.isWritePipe());
//						System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");						

						
						
						// gssg - xl t2t 지원
						// gssg - t2t bulk mode 스레드 순서 조정
						// gssg - o2t 지원
						// gssg - o2t bulk mode 지원
//						if ( this.jobRunPol.isWritePipe() ) {
//						if ( sLine.length() != 0 && sLine.charAt(0) == 'S' ) {							
//						this.jobRunPol.setLoadQuery(true);
//						XLLogger.outputInfoLog("@@@@@@@@@@@@@@@@@@[GSSG DEBUG] THIS POINT IS AFTER this.jobRunPol.setLoadQuery(true);@@@@@@@@@@@@@@@@@@@");							
//						XLLogger.outputInfoLog("@@@@@@@@@@@@@@@@@@[GSSG DEBUG] THIS POINT IS AFTER sLine.charAt(0) == 'S' @@@@@@@@@@@@@@@@@@@");
//						}
					}


					
					// XLLogger.outputInfoLog("CKSOHN DEBUG::: SQLLDR RESULT = " + vwResultInfo.toString());
				} catch (Exception ee) {
					ee.printStackTrace();
					
				} finally {
					try { if ( reader != null ) reader.close(); } catch (Exception e) {} finally { reader = null;}
				}
							
//				XLLogger.outputInfoLog("@@@@@@@@@@@@@@@@@@[GSSG DEBUG] THIS POINT IS AFTER while((s = reader.readLine()) != null);@@@@@@@@@@@@@@@@@@@");								
			
			pr.waitFor();

//			XLLogger.outputInfoLog("@@@@@@@@@@@@@@@@@@[GSSG DEBUG] THIS POINT IS AFTER pr.waitFor();@@@@@@@@@@@@@@@@@@@");
			
			XLLogger.outputInfoLog(this.logHead + " Finished ApplyLoaderThread(BULK MODE)");
			
		} 		
		
		
		catch (InterruptedException ie) { // cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 - 추가 처리
			
			XLLogger.outputInfoLog("[WARN] Loader is Interrupted." );
			
			this.jobRunPol.setStopJobFlag(true);
			
			// errMsg = ie.toString();
			errMsg = "Loader Thread(Direct Path mode) is stopped by interruption.";
			
			XLLogger.outputInfoLog(this.logHead + "Loader Thread(Direct Path mode) is stopped by interruption.");
			
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_ABORT);
			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.setErrMsg_Loader(this.logHead +"[EXCEPTION] " +  errMsg);
			

			
		} 
		
		catch (Exception e) {
			
			XLException.outputExceptionLog(e);
			
			this.jobRunPol.setStopJobFlag(true);
			
			errMsg = e.toString();
			XLLogger.outputInfoLog(this.logHead + "[EXCEPTION] Loader Thread(Direct Path mode) is stopped abnormal.");
			
			this.jobRunPol.setJobStatus(XLOGCons.STATUS_FAIL);
			// cksohn - BULK mode oracle sqlldr
			this.jobRunPol.setErrMsg_Loader(this.logHead +"[EXCEPTION] " +  errMsg);
						
			
		} finally {
			
			// cksohn - BULK mode oracle sqlldr
			
			// cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정
			this.jobRunPol.setRunLoader(false);
			
//			XLLogger.outputInfoLog("[GSSG DEBUG] THIS POINT IS AFTER this.jobRunPol.setRunLoader(false););");

			
			// cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발생 오류 수정
			try { pr.destroy(); } catch (Exception ee) {}
						

		}
		
	}
	

}
