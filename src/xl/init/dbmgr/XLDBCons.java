package xl.init.dbmgr;

public class XLDBCons {
	
	public static final String DATE_FORMAT_ALTI = "YYYY-MM-DD HH24:MI:SS";	// ALTIBASE date type ��¥ ����
	public static final String TIMESTAMP_FORMAT_ALTI = "YYYY-MM-DD HH24:MI:SS.SSSSSS";	// ALTIBASE TIMESTAMP ����
	// cksohn - for sundb
	public static final String DATE_FORMAT_SUNDB = "YYYY-MM-DD HH24:MI:SS";	
	public static final String TIMESTAMP_FORMAT_SUNDB = "YYYY-MM-DD HH24:MI:SS.FF6";	
	public static final String DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS";	// ����Ŭ date type ��¥ ����
	//3.2.00-015 2011-07-11 modify for nls_timestamp format
	public static final String TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SSXFF";	// ����Ŭ TIMESTAMP ����
	// sykim 3.2.01-037 2013-03-06 for timestamp zone format
	public static final String TIMESTAMP_TZ_FORMAT = "YYYY-MM-DD HH24:MI:SSxFF TZH:TZM";

}
