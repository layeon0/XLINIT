package xl.init.info;

import xl.lib.common.XLCons;


/**
 * 
 * @author cksohn
 * 
 * XL_DBMS 테이블 정보
 *
 */
public class XLDBMSInfo {
	
	// PK
	private String 	ip = "";
	private String 	dbSid = "";
	
	// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
	private String 	dbServiceName = "";
	
	private int 	port = 1521;
	// private String 	type = "ORACLE";
	private String	dbTypeStr = "ORACLE";
	private byte 	dbType = XLCons.ORACLE;
	
	private String 	userId = "xladmin";
	private String 	passwd = "xladmin";
	
	private int 	cpuThreshold = 100; // %
	private int 	cpuThresholdTime = 600; // sec
	
	private int 	maxJobCnt = 1; // DBMS 서버별 동시작업 가능 최대갯수
	
	// gssg - damo 캐릭터셋 하드 코딩 보완
	// gssg - 캐릭터셋 컬럼 데이터 처리
	private String 	charSet = "UTF8";
	private String 	nCharSet = "UTF8";
	


	// gssg - damo 캐릭터셋 하드 코딩 보완
	// gssg - 캐릭터셋 컬럼 데이터 처리	
//	public XLDBMSInfo(String ip, String dbSid, int port, String dbTypeStr, byte dbType, String userId, String passwd, int cpuThreshold,
//			int cpuThresholdTime, int maxJobCnt) {
	// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
	public XLDBMSInfo(String ip, String dbSid, String dbServiceName, int port, String dbTypeStr, byte dbType, String userId, String passwd,  String charSet, String nCharset) {

		super();
		this.ip = ip;
		this.dbSid = dbSid;
		
		// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록
		this.dbServiceName = dbServiceName;
		
		this.port = port;
		this.dbTypeStr = dbTypeStr;
		this.dbType = dbType;
		this.userId = userId;
		this.passwd = passwd;
		//this.cpuThreshold = cpuThreshold;
		//this.cpuThresholdTime = cpuThresholdTime;
		//this.maxJobCnt = maxJobCnt;
		
		// gssg - damo 캐릭터셋 하드 코딩 보완
		// gssg - 캐릭터셋 컬럼 데이터 처리	
		this.charSet = charSet;
		this.nCharSet = nCharset;

		
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getDbSid() {
		return dbSid;
	}

	public void setDbSid(String dbSid) {
		this.dbSid = dbSid;
	}
	
	
	// cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록 start - [
	public  String getDbServiceName() {
		return dbServiceName;
	}

	public  void setDbServiceName(String dbServiceName) {
		this.dbServiceName = dbServiceName;
	}
	// ] - end cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}
	
	

	public String getDbTypeStr() {
		return dbTypeStr;
	}

	public void setDbTypeStr(String dbTypeStr) {
		this.dbTypeStr = dbTypeStr;
	}

	public byte getDbType() {
		return this.dbType;
	}

	public void setDbType(byte dbType) {
		this.dbType = dbType;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getPasswd() {
		return passwd;
	}

	public void setPasswd(String passwd) {
		this.passwd = passwd;
	}

	public int getCpuThreshold() {
		return cpuThreshold;
	}

	public void setCpuThreshold(int cpuThreshold) {
		this.cpuThreshold = cpuThreshold;
	}

	public int getCpuThresholdTime() {
		return cpuThresholdTime;
	}

	public void setCpuThresholdTime(int cpuThresholdTime) {
		this.cpuThresholdTime = cpuThresholdTime;
	}

	public int getMaxJobCnt() {
		return maxJobCnt;
	}

	public void setMaxJobCnt(int maxJobCnt) {
		this.maxJobCnt = maxJobCnt;
	}

	// gssg - damo 캐릭터셋 하드 코딩 보완
	// gssg - 캐릭터셋 컬럼 데이터 처리	-- start
	public String getCharSet() {
		return charSet;
	}

	public void setCharSet(String charSet) {
		this.charSet = charSet;
	}

	public String getnCharSet() {
		return nCharSet;
	}

	public void setnCharSet(String nCharSet) {
		this.nCharSet = nCharSet;
	}
	
	// -- end
	
	
	
}
