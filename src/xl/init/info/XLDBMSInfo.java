package xl.init.info;

import xl.lib.common.XLCons;


/**
 * 
 * @author cksohn
 * 
 * XL_DBMS ���̺� ����
 *
 */
public class XLDBMSInfo {
	
	// PK
	private String 	ip = "";
	private String 	dbSid = "";
	
	// cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���
	private String 	dbServiceName = "";
	
	private int 	port = 1521;
	// private String 	type = "ORACLE";
	private String	dbTypeStr = "ORACLE";
	private byte 	dbType = XLCons.ORACLE;
	
	private String 	userId = "xladmin";
	private String 	passwd = "xladmin";
	
	private int 	cpuThreshold = 100; // %
	private int 	cpuThresholdTime = 600; // sec
	
	private int 	maxJobCnt = 1; // DBMS ������ �����۾� ���� �ִ밹��
	
	// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
	// gssg - ĳ���ͼ� �÷� ������ ó��
	private String 	charSet = "UTF8";
	private String 	nCharSet = "UTF8";
	


	// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
	// gssg - ĳ���ͼ� �÷� ������ ó��	
//	public XLDBMSInfo(String ip, String dbSid, int port, String dbTypeStr, byte dbType, String userId, String passwd, int cpuThreshold,
//			int cpuThresholdTime, int maxJobCnt) {
	// cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���
	public XLDBMSInfo(String ip, String dbSid, String dbServiceName, int port, String dbTypeStr, byte dbType, String userId, String passwd,  String charSet, String nCharset) {

		super();
		this.ip = ip;
		this.dbSid = dbSid;
		
		// cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���
		this.dbServiceName = dbServiceName;
		
		this.port = port;
		this.dbTypeStr = dbTypeStr;
		this.dbType = dbType;
		this.userId = userId;
		this.passwd = passwd;
		//this.cpuThreshold = cpuThreshold;
		//this.cpuThresholdTime = cpuThresholdTime;
		//this.maxJobCnt = maxJobCnt;
		
		// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
		// gssg - ĳ���ͼ� �÷� ������ ó��	
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
	
	
	// cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ��� start - [
	public  String getDbServiceName() {
		return dbServiceName;
	}

	public  void setDbServiceName(String dbServiceName) {
		this.dbServiceName = dbServiceName;
	}
	// ] - end cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ���

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

	// gssg - damo ĳ���ͼ� �ϵ� �ڵ� ����
	// gssg - ĳ���ͼ� �÷� ������ ó��	-- start
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
