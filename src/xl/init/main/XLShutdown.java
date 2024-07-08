package xl.init.main;

import xl.init.logger.XLLogger;
import xl.lib.msgpacket.XLMsgPacket;
import xl.lib.msgpacket.XLOPCode;
import xl.init.conf.XLConf;
import xl.init.info.XLMemInfo;
import xl.init.util.XLUtil;

public class XLShutdown {
	
	
    
	public XLShutdown() {
	}
	 
	
	public static void shutdown()
	{
		try {
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog("X-LOG Manager shutdown processing start....[Runnging JOB : " + XLMemInfo.HT_RUNNING_JOB_INFO.size() + "]");
			
			// 1. STOP_FLAG setting
			XLInit.STOP_FLAG = true;
			
			// 2. 현재 Running 중인 JOB이 모두 종료/중지 될때까지 Waiting
			while ( XLMemInfo.HT_RUNNING_JOB_INFO.size() != 0 ) {
				XLLogger.outputInfoLog("Waiting Running Job is finished before Shutdown....");
				Thread.sleep(3000);				
			}
			
			XLLogger.outputInfoLog("");
			XLLogger.outputInfoLog("X-LOG Manager shutdown is completed");
			
			System.exit(0);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}


	// public static void sendShutdownCmd(String pol_code, int port, boolean isForceStop){
	public static void sendShutdownCmd(int port){
		XLMsgPacket msgPacket = new XLMsgPacket();
		byte bt_cmd = XLOPCode.XL_SHUTDOWN;

		msgPacket.setOpCode(bt_cmd);
		// msgPacket.setPolicyCode(pol_code);
		msgPacket.setPolicyCode("");
		
		XLUtil.sendCommand(msgPacket, "127.0.0.1", port);
	}
	

	public static void main(String[] args) {
		
		
		// cksohn - scheduler 1.0 최초 버전 수정
		XLInit manager = new XLInit();
		manager.init();

		
		String sCmd = "";
		
		// if( args.length < 3 ){
		if( args.length < 1 ){ // cksohn - scheduler 1.0 최초 버전 수정
			System.out.println("usage : xlmgrd shutdown");
			return;
		}
				
		sCmd = args[0];
		if( !sCmd.equalsIgnoreCase( "shutdown" ) ) {
			System.out.println("usage : xlmgrd shutdown");
		}
		
		int port = XLConf.XL_MGR_PORT;

		if ( port != -1 ) {
			sendShutdownCmd(port);
		} else {
			System.out.println("[EXCEPTION] Manager port = " + port);
		}
	}
}
