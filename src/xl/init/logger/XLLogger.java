package xl.init.logger;



import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.StringTokenizer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import xl.init.conf.XLConf;
import xl.init.logger.XLLogger;

public class XLLogger {
	
		
	/** LogLevel OFF */
	public static final int LEVEL_OFF   = 1;
	/** LogLevel INFO */
	public static final int LEVEL_INFO  = 2;	
	/** LogLevel DEBUG */
	public static final int LEVEL_DEBUG = 3;	
	/** LogLevel ERROR */
	public static final int LEVEL_ERROR = 4;
	/** LogLevel FATAL */
	public static final int LEVEL_FATAL = 5;
	/** LogLevel ALL */
	public static final int LEVEL_ALL   = 6;
	
	//Console Log Pattern을 지정해 주세요..
	//ex1://"%-5r [Severity:%-5p][%d{yyyy/MM/dd HH:mm:ss SSS}][Thread:%-5t][Location:%C.%M] %-30m%n";
	//public static final String DEFAULT_LOG_PATTERN = "[%t]%3p:%C{1}.%M(): %m%n";
	//public static final String DEFAULT_LOG_PATTERN = "[%C{1}.%M()] %m%n";
	
	//File Log Pattern을 지정해 주세요..
	//public static final String DEFAULT_FILELOG_PATTERN = "%-3r[%d{yyMMddHHmmss}][%t]%3p:%C{1}.%M(): %m%n";
	//public static final String DEFAULT_FILELOG_PATTERN = "[%d{yy/MM/dd HH:mm:ss}] [%C{1}.%M()] %m%n";
	public static final String DEFAULT_FILELOG_PATTERN = "[%d{yy/MM/dd HH:mm:ss}] %m%n";
	public static final String DEFAULT_LOG_FILENAME = "xl_sched.log";
	
	// public static Logger out = Logger.getRootLogger();
	// cksohn - Logger 중복 로깅 수정
	public static Logger out = Logger.getLogger("INITLogger");
	
	private static RollingFileAppender _FileAppender = null;
	private static int _logLevel = XLConf.XL_LOG_LEVEL;
	private static String xl_log_conf = XLConf.XL_LOG_CONF;
	
	
	/** init logger by default(OutputPath is current directory. consoleEnabled,dailyFileEnable,immediateFlush are all true) */
	public static void init() {
		init(null,null);
	}
	
	/** init logger by default(consoleEnabled,dailyFileEnable,immediateFlush are all true) */
	public static void init(String outputPath, String fileName) {
		//cksohn - Logger 중복 로깅 수정
		out.setAdditivity(false);
		
		init(outputPath,DEFAULT_FILELOG_PATTERN, 
			fileName,  
			true, true, _logLevel,xl_log_conf);
	}
	
	/** init logger */
	public static void init(String outputPath,String fileLogPattern,
		String logFileName, boolean FileEnable,
		boolean immediateFlush, int logLevel, String logConf)
	{
		setFilaEnabled(false);
		
		_FileAppender = null;
			
		//fix outputPath
		if (outputPath == null) {
			outputPath = System.getProperty("user.dir");
		}
		else {
			File file = new File(outputPath);
			if (!file.exists()) {
				XLLogger.out.debug("NRCapLogger.init(): outputPath("+outputPath+") is NOT exist");
				outputPath = System.getProperty("user.dir");
			}
		}
		
		//fix logFileName
		if (logFileName == null || logFileName.length() == 0) logFileName = DEFAULT_LOG_FILENAME;
		//fix fileLogPattern
		if (fileLogPattern == null || fileLogPattern.length() == 0) fileLogPattern = DEFAULT_FILELOG_PATTERN;
		//fix logPattern
		try {
			if (FileEnable) {
				_FileAppender =
					new RollingFileAppender(
						new PatternLayout(fileLogPattern),
						outputPath + File.separator + logFileName);
			}
		} catch (IOException e) {
			e.printStackTrace();
			XLLogger.out.debug("NRCapLogger Init Fail.");
		}
		
		setMaxFile(logConf);
		setLevel(logLevel);
		setImmediateFlush(immediateFlush);
		setFilaEnabled(FileEnable);
	}
	
	/** set immediately flush */
	public static void setImmediateFlush(boolean b) {
		if (_FileAppender != null) _FileAppender.setImmediateFlush(b);
	}
		
	/** set file logger enable/disable
	 * (enable은 최초 init시 consoleEnabled=true인 경우만 허용됨) */
	public static void setFilaEnabled(boolean enable) {
		if (_FileAppender != null) {
			if (enable) out.addAppender(_FileAppender);
			else out.removeAppender(_FileAppender);
		}
	}
	
	/** get log level */
	public static int getLevel() {
		return _logLevel;
	}
	
	/** set log level */
	public static void setLevel(int logLevel) {
		switch(logLevel) {
			case LEVEL_OFF:
				setLevelOff();
				break;
			case LEVEL_FATAL:
				setLevelFatal();
				break;
			case LEVEL_ERROR:
				setLevelError();
				break;
			case LEVEL_DEBUG:
				setLevelDebug();
				break;
			case LEVEL_INFO:
				setLevelInfo();
				break;
			case LEVEL_ALL:
				setLevelAll();
				break;
		}
	}
	
	/** OFF */
	public static void setLevelOff() {
		_logLevel = LEVEL_OFF;
		out.setLevel(Level.OFF);
	}
	
	/** set log level INFO */
	public static void setLevelInfo() {
		_logLevel = LEVEL_INFO;
		out.setLevel(Level.INFO);
	}
	
	/** set log level ERROR */
	public static void setLevelError() {
		_logLevel = LEVEL_ERROR;
		out.setLevel(Level.ERROR);
	}
	
	/** set log level DEBUG */
	public static void setLevelDebug() {
		_logLevel = LEVEL_DEBUG;
		out.setLevel(Level.DEBUG);
	}
	
	/** set log level FATAL */
	public static void setLevelFatal() {
		_logLevel = LEVEL_FATAL;
		out.setLevel(Level.FATAL);
	}
	
	/** set log level ALL */
	public static void setLevelAll() {
		_logLevel = LEVEL_ALL;
		out.setLevel(Level.ALL);
	}
	
	/** get stack trace info string */
	public static String getStackTraceInfo(Throwable t) {
		StringWriter sw = new StringWriter();
		PrintWriter writer = new PrintWriter(sw, true);
		t.printStackTrace(writer);
		return sw.toString();
	}
	public static void setMaxFile(String _logConf){
		StringTokenizer st = new StringTokenizer(_logConf, ",");
		String maxFileSize="100";
		int backupIndex=10;
		if(st.countTokens() == 2){
			maxFileSize=st.nextToken();
			backupIndex=Integer.parseInt(st.nextToken());
		}
		_FileAppender.setMaxFileSize(maxFileSize+"MB");
		_FileAppender.setMaxBackupIndex(backupIndex);
	}
	
	// cksohn - scheduler 1.0 최초 버전 수정
	synchronized static public void outputInfoLog(Object _msg) 
	{
		XLLogger.out.info(_msg); 
		// XLLogger.out.warn(_msg);
	}
}