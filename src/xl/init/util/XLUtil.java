package xl.init.util;

import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.commons.codec.binary.Hex;

import oracle.net.nt.SdpNTAdapter;
import xl.lib.common.XLCons;
import xl.lib.msgpacket.XLMsgPacket;
import xl.init.conf.XLConf;
import xl.init.info.XLDBMSInfo;
import xl.init.info.XLDicInfoCons;
import xl.init.info.XLJobColInfo;
import xl.init.info.XLJobRunPol;
import xl.init.info.XLJobTableInfo;
import xl.init.logger.XLLogger;
import xl.init.main.XLInit;

public class XLUtil {
	
    public static XLMsgPacket sendCommand( XLMsgPacket _obj , String ipaddr, int port)
    {    	
    	Socket socket;

        OutputStream    outStream   = null;
        InputStream     inStream    = null;
        ObjectOutputStream    out	= null;  
        ObjectInputStream     in	= null;
                
        try{
        	
        	socket = new Socket(ipaddr, port); 
			outStream = socket.getOutputStream();
			out = new ObjectOutputStream(outStream);
			inStream = socket.getInputStream();			
			in = new ObjectInputStream( inStream );
			
			out.writeObject( _obj );
			out.flush();

			if(in!=null){
				return (XLMsgPacket)in.readObject();
			}else{
				return null;
			}
        } catch(Exception e) {
        	e.printStackTrace();
        	return null;
        }
    }
        
  
	
	public static byte getDBMSType(String _type){
		// cksohn - for Mysql Catalog DB  - cksohn 3.5.00-001 2015-02-06 - Informix source DB - catalog DB
		// equals --> equalsIgnoreCase 로 변경
		if(_type.equalsIgnoreCase("ORACLE")){
			return XLCons.ORACLE;
		} else if ( _type.equalsIgnoreCase("INFORMIX") ) { // // cksohn 3.5.00-001 2015-02-06 - Informix to Informix - Informix Catalog DB
			return XLCons.INFORMIX;
		}else if(_type.equalsIgnoreCase("MYSQL")){
			return XLCons.MYSQL;
		}else if(_type.equalsIgnoreCase("MARIADB")){ // gssg - xl MariaDB 동기화
			return XLCons.MARIADB;
		}else if(_type.equalsIgnoreCase("ALTIBASE")){ // cksohn - for Altibase porting
			return XLCons.ALTIBASE;
//		}else if(_type.equalsIgnoreCase("SUNDB")){ // cksohn - for sundb
//			return XLCons.SUNDB;			
			// cksohn SUNDB-->GOLDILOCKS
		}else if(_type.equalsIgnoreCase("ALTIBASE5")){ // gssg - xl MariaDB 동기화
			// gssg - 국가정보자원관리원 데이터이관 사업
			// gssg - Altibase5 to Altibase7 지원
			return XLCons.ALTIBASE5;
		}else if(_type.equalsIgnoreCase("GOLDILOCKS")){ // cksohn - for sundb
			return XLCons.GOLDILOCKS;
		} else if (_type.equalsIgnoreCase("PPAS") ) { // cksohn - for otop
			return XLCons.PPAS;
		} else if (_type.equalsIgnoreCase("POSTGRESQL") ) {
			// gssg - xl 전체적으로 보완2
			// gssg - PostgreSQL 커넥터 처리
			return XLCons.POSTGRESQL;
		} else if (_type.equalsIgnoreCase("TIBERO")){ // cksohn - for otot
			return XLCons.TIBERO;			
		} else if (_type.equalsIgnoreCase("MSSQL")){ // cksohn - for otot
			// gssg - ms2ms 지원
			// gssg - MSSQL DBMS 추가
			return XLCons.MSSQL;
		} else if (_type.equalsIgnoreCase("CUBRID")){
			// gssg - 국가정보자원관리원 데이터이관 사업
			// gssg - Cubrid to Cubrid 지원
			return XLCons.CUBRID;
		} else{
			return XLCons.ORACLE;
		}		
	}
	
	//sykim 3.2.01-019 2012-04-18 for table name dot - start
	public static String checkTblName(String tbname){
		if(tbname.contains(".")||tbname.contains(",")){
			tbname = "\"" + tbname + "\""; 
		}
		return tbname;
	}
	//sykim 3.2.01-019 2012-04-18 for table name dot - end
	



	// // cksohn - avoid data loss when apply kill -9
	public static String removeDot( String _str ) {
		try {
			if ( _str != null ) {
				int dotIdx = _str.indexOf(".");
				if ( dotIdx != -1 ) {
					_str = _str.substring(0, dotIdx);
				}
				return _str;
			} else {
				return _str;
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			return _str;
		}
	}
	
	
	// gssg - xl p2t 지원
	// gssg - p2t 하다가 o2m time zone 처리
	public static String removeTZ(String _tzValue) {
		
		// TZ : 2022-09-27 16:41:52.868549 +9:00		->			2022-09-27 16:41:52.868549
		// LTZ : 2022-09-27 16:41:51.447679 Asia/Seoul		->			2022-09-27 16:41:51.447679
		
			try {
				if ( _tzValue != null ) {
					_tzValue = _tzValue.substring(0, _tzValue.lastIndexOf(" "));					
					return _tzValue;
				} else {
					return _tzValue;
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				return _tzValue;
			}	
		}	
	
	
	// gssg - xl p2t 지원
	// gssg - p2t 하다가 o2m time zone 처리
	public static String removeMySQLTZ(String _tzValue) {
		
		// TZ : 2022-09-27 16:41:52.868549 +9:00		->			2022-09-27 16:41:52.868549
		// LTZ : 2022-09-27 16:41:51.447679 Asia/Seoul		->			2022-09-27 16:41:51.447679
		
			try {
				if ( _tzValue != null ) {
					_tzValue = _tzValue.substring(0, _tzValue.lastIndexOf(" "));					
					return _tzValue;
				} else {
					return "\\N"; // mysql은 \N 이 null 로 들어감
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				return _tzValue;
			}	
		}

	
	// gssg - 국가정보자원관리원 데이터이관 사업
	// gssg - Oracle to Oracle 타임존 처리
	public static String removeOracleTZ(String _tzValue) {
		
		// TZ : 2022-09-27 16:41:52.868549 +9:00		->			2022-09-27 16:41:52.868549
		// LTZ : 2022-09-27 16:41:51.447679 Asia/Seoul		->			2022-09-27 16:41:51.447679
		
			try {
				if ( _tzValue != null ) {
					_tzValue = _tzValue.substring(0, _tzValue.lastIndexOf(" "));					
					return _tzValue;
				} else {
					return ""; // oracle은 "" 이 null 로 들어감
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				return _tzValue;
			}	
		}

	
	
	// cksohn - SEQ 복제 일반정책에서 가능하도록 보완
	public static String removeLastSemicoln(String _ddlStr) {
		String ddlStr = "";
		try {
			
			if ( _ddlStr != null ) {
				
				if ( _ddlStr.trim().endsWith(";") ) {
					ddlStr = _ddlStr.substring(0, _ddlStr.trim().length()-1);
				} else {
					ddlStr = _ddlStr;
				}
			}
			return ddlStr;
			
		} catch (Exception e) {
			e.printStackTrace();
			return _ddlStr;
		}
	}
	

	// cksohn - DDL "" 처리 이슈
	// remove "" - "TABLE_NAME" --> TABLE_NAME
	public static String removeDoblueQuat(String _strValue){
		
		try {
		
			
			String str = "";

			StringTokenizer st = new StringTokenizer(_strValue, "\"");
			
			str = st.nextToken();
			
			return str;			
			
		} catch (Exception e) {
			System.out.println("[EXP]" + e.toString());
			return _strValue;
		}
		
	}
	
	// cksohn - kill -9 성능 개선  - to disk log
	// file의 line 수 return
    public static int getLineCnt(String _filePath)
    {

		FileReader fr = null;
		BufferedReader br = null;
		String strLine = null;
		
		int lineCnt = 0;
		try{
			File file = new File(_filePath);
			if ( file.exists() ) {
				fr = new FileReader(_filePath);			
				br = new BufferedReader(fr);
				
				while( (strLine=br.readLine()) != null ){
					lineCnt++;
				}	
	
				return lineCnt;
			} else {
				return 0;
			}
		}catch(Exception e){
			e.printStackTrace();
			return -1;
		}finally{
			try{if(br!=null)br.close();}catch(Exception e){}finally{br = null;}
			try{if(fr!=null)fr.close();}catch(Exception e){}finally{fr = null;}
		}		

    }

    
	// hex to byte[] 	
	public static byte[] hexToByteArray(String hex) { 
	    if (hex == null || hex.length() == 0) { 
	        return null; 
	    } 

	    // cksohn - hexToBytesArray() 성능 개선 start - [
	    try {
	    	return Hex.decodeHex(hex.toCharArray());	    	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	return null;
	    }
	    // ] - end // cksohn - hexToBytesArray() 성능 개선
	}
	
	public static String bytesToHexString(byte[] bytes) {
		
		try {
			
			return Hex.encodeHexString(bytes);
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	} 
	
	
	// 현재 날짜 --> String format yyyy-MM-dd HH:mm:ss
	public static String getCurrentDateStr()
	{
		try {
			
			Date curDate = new Date();

			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String dateStr = sdf.format(curDate);
			
			return dateStr;
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	
	// cksohn - BULK mode oracle sqlldr
	// public static String makePipe(String _dirPath, long _jobSeq)
	public static String makePipe(String _polName, String _tableName)
	{
		try {
			
			// 일단은 linux 기준으로. TODO: Windows
			// 1. 파일네임 생성 및 파일 존재 여부 check
			// String pipeName = makePipeName(_dirPath, _srcOrTar);
			String pipeName = _polName + "_" + _tableName + "_pipe";
			
			if ( pipeName != null ) {
				
				File file = new File(XLInit.XL_DIR + File.separator + "pipe" + File.separator + pipeName);
				if ( file.exists() ) {
					// 존재하면 삭제
					// XLFileInitManager.outputInfoLog("CKSOHN DEBUG:::: DELETE PIPE : " + file.getAbsolutePath());
					file.delete();
				}
				
				// ~/pipe/ directory 없으면 mkdir
				String pDir = file.getParent();
				File pDirF = new File(pDir);
				if ( !pDirF.exists() ) {
					pDirF.mkdirs();
				}
				
				// 파일 생성 
				Process pr = null;				
				pr = Runtime.getRuntime().exec("mkfifo -m 600 " + file.getAbsolutePath());
				pr.waitFor();
				
				pipeName = file.getAbsolutePath();
				
				return pipeName;		
				
			} else {
				
				return null;
				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// gssg - csv file create 기능 추가
	// gssg - csv 파일 생성
	public static String makeCSV(XLJobRunPol _jopRunPol)
	{
		try {
			
			XLJobTableInfo jobTableInfo = _jopRunPol.getTableInfo();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			Date now = new Date();
			String nowTime = sdf.format(now);
			
			// gssg - csv file create 기능 추가
			// gssg - file 생성이 완료되면 확장자 변경
			String csvName = jobTableInfo.getSowner() + "-" + jobTableInfo.getStable() + "-" + nowTime + ".tmp";
			
			if ( csvName != null ) {
				
				File file = new File(XLConf.XL_CREATE_FILE_PATH + File.separator + csvName);
				
				// directory 없으면 mkdir
				String cDir = file.getParent();
				File cDirF = new File(cDir);
				if ( !cDirF.exists() ) {
					cDirF.mkdirs();
				}

				// 파일 생성 
				if ( file.createNewFile() ) {
					XLLogger.outputInfoLog("[CSV FILE CREATE] File created");
				} else {
					XLLogger.outputInfoLog("[CSV FILE CREATE] File already exists");					
				}
												
				csvName = file.getAbsolutePath();
				
				return csvName;		
				
			} else {
				
				return null;
				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// gssg - csv file create 기능 추가
	// gssg - file 생성이 완료되면 확장자 변경
	public static void renameFile(String csvFilePath) {
		try {
						
			File oldfile = new File(csvFilePath);
						
			csvFilePath = csvFilePath.substring(0, csvFilePath.length() - 3) + XLConf.XL_CREATE_FILE_EXTENSION;			
			
			File newfile = new File(csvFilePath);

			if ( oldfile.renameTo(newfile) ) {
				XLLogger.outputInfoLog("[CSV FILE CREATE] File rename success");	
			} else {
				XLLogger.outputInfoLog("[CSV FILE CREATE] File rename fail");	
			}			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	
	// cksohn - BULK mode oracle sqlldr
	/**
	 * 
	 * @param _jobSeq
	 * @param _tableInfo
	 * @return
	 * 
	 * [ 일반 타입 sample ]
	 * LOAD DATA CHARACTERSET UTF8 
			INFILE '/home/XLOG/OtoO_TTA/XLManager/csval/PTA101/TESTUSER.XL_VER_TEST01' "STR '|'"
			TRUNCATE INTO TABLE TESTUSER.XL_VER_TEST01
			FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
			TRAILING NULLCOLS
			("ID", 
			"NAME" CHAR(4000), 
			"ADDR" CHAR(4000), 
			"REGDATE" TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF")
			
	   [ LOB 컬럼 sample ]
		LOAD DATA CHARACTERSET UTF8 
			INFILE '/home/XLOG/OtoO_TTA/XLManager/csval/PTA101/TESTUSER.XL_LOB_TEST01' "STR '|'"
			TRUNCATE INTO TABLE TESTUSER.XL_LOB_TEST01
			FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
			TRAILING NULLCOLS
			("ID", 
			"NAME" CHAR(4000), 
			"B_BLOB" CHAR(1000000)NULLIF ("B_BLOB"=X'FF'), 
			"AGE", 
			"C_CLOB" CHAR(1000000)NULLIF ("C_CLOB"=X'FF'), 
			"ADDR" CHAR(4000), 
			"REGDATE" TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF")

	 */	
	public static String makeCtlFile_ORACLE(XLJobRunPol _jobRunPol, String _tableName)
	{
		try {
			
			// 일단은 linux 기준으로. TODO: Windows
			// 1. 파일네임 생성 및 파일 존재 여부 check
			// String pipeName = makePipeName(_dirPath, _srcOrTar);
			String ctlName = _jobRunPol.getPolName() + "_" + _tableName + ".ctl";
			
			if ( ctlName != null ) {
				
				File file = new File(XLInit.XL_DIR + File.separator + "pipe" + File.separator + ctlName);
				if ( file.exists() ) {
					// 존재하면 삭제
					// XLFileInitManager.outputInfoLog("CKSOHN DEBUG:::: DELETE PIPE : " + file.getAbsolutePath());
					file.delete();
				}
				
				// ~/pipe/ directory 없으면 mkdir
				String pDir = file.getParent();
				File pDirF = new File(pDir);
				if ( !pDirF.exists() ) {
					pDirF.mkdirs();
				}
				
				// full path set
				ctlName = file.getAbsolutePath();
				
				XLJobTableInfo jobTableInfo = _jobRunPol.getTableInfo();
				// 파일 생성
				StringBuffer sb = new StringBuffer();
				
				
				sb.append("LOAD DATA CHARACTERSET UTF8\n")
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' \"STR '|'\"\n")
				
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("'\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
					.append("INFILE '").append(_jobRunPol.getBulk_pipePath()).append("' ").append(XLConf.XL_BULK_ORACLE_EOL_CTL_FORMAT).append("\n")
					
					
					
					// .append("TRUNCATE INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					
					
					// .append("APPEND INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
					// gssg - 국가정보자원관리원 데이터이관 사업
					// gssg - Altibase to Oracle 지원
//					.append("INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					.append("INTO TABLE \"").append((jobTableInfo.getTowner()).toUpperCase() + "\".\"" + (jobTableInfo.getTtable()).toUpperCase() + "\"\n")
					.append("APPEND\n")
					
					
					//.append("INSERT INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					.append("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n ")
					.append("TRAILING NULLCOLS\n")
					.append("(");
				
				// 컬럼 정보 settting
				Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();
				for ( int i=0; i<vtJobColInfo.size(); i++ ) {
					
					if ( i != 0 ) {
						sb.append(",\n");
					}
					
					XLJobColInfo colInfo = vtJobColInfo.get(i);
					
					sb.append("\"" + (colInfo.getColName_map()).toUpperCase() + "\""); // 컬럼명
					
					switch ( colInfo.getDataType()) {
					
						case XLDicInfoCons.CHAR:
						case XLDicInfoCons.NCHAR:
						case XLDicInfoCons.VARCHAR:
						case XLDicInfoCons.VARCHAR2:
							// gssg - 대법원 O2O
							// gssg - raw_to_varchar2 기능 지원												
//							sb.append(" CHAR(4000)");
							sb.append(" CHAR(8000)");
							break;

						// gssg - LG엔솔 MS2O
						// gssg - ms2o bulk mode 지원
						case XLDicInfoCons.SMALLDATETIME:	
						case XLDicInfoCons.DATE:
							// gssg - 국가정보자원관리원 데이터이관 사업
							// gssg - Altibase to Oracle 지원
							// gssg - Altibase5 to Altibase7 지원
							if ( _jobRunPol.getSdbInfo().getDbType() == XLCons.ALTIBASE || _jobRunPol.getSdbInfo().getDbType() == XLCons.ALTIBASE5 ) {
								sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");								
							} else {
								sb.append(" DATE \"YYYY-MM-DD HH24:MI:SS\"");								
							}
							
							break;
							
						// gssg - LG엔솔 MS2O
						// gssg - ms2o bulk mode 지원
						case XLDicInfoCons.DATETIME:
						case XLDicInfoCons.DATETIME2:							
						case XLDicInfoCons.TIMESTAMP:
							sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");
							break;
						
						// gssg - 국가정보자원관리원 데이터이관 사업
						// gssg - Oracle to Oracle 타임존 처리
						// gssg - LG엔솔 MS2O
						// gssg - ms2o bulk mode 지원
						// gssg - o2o bulk tz 보완
						case XLDicInfoCons.DATETIMEOFFSET:
						case XLDicInfoCons.TIMESTAMP_TZ:
							// "T" "TO_TIMESTAMP_TZ(:T,'YYYY-MM-DD HH24:MI:SS:FF TZHTZM')")
//							sb.append(" \"TO_TIMESTAMP_TZ(:" + colInfo.getColName_map() + ", 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM')\"");
							sb.append(" \"TO_TIMESTAMP_TZ(:" + colInfo.getColName_map() + ", 'YYYY-MM-DD HH24:MI:SS.FF TZR')\"");

							break;
						
						// gssg - LG엔솔 MS2O
						// gssg - o2o bulk ltz 보완
						case XLDicInfoCons.TIMESTAMP_LTZ:
							sb.append(" \"FROM_TZ(TO_TIMESTAMP(:" + colInfo.getColName_map() + ", 'YYYY-MM-DD HH24:MI:SS.FF'), '" + XLConf.XL_TIMEZONE + "')\"");
							break;
						
							
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.BLOB:
							sb.append(" CHAR(1000000)NULLIF (\"" + colInfo.getColName_map() + "\"=X'FF')");
							break;							
							
					}
										
					// gssg - xl function 기능 지원
					// gssg - oracle bulk mode function 지원
					if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
						sb.append(" \"" + colInfo.getFunctionStr() + "(:" + (colInfo.getColName_map()).toUpperCase() + ")\"");
					}					
				} // for-end
				
				// gssg - LG엔솔 MS2O
				// gssg - 공장코드 값 처리
				if ( _jobRunPol.getCustomValue() != null && !_jobRunPol.getCustomValue().equals("") ) {
					sb.append(",\n\"" + _jobRunPol.getCustomColname() + "\" CONSTANT \"" + _jobRunPol.getCustomValue() + "\"");										
				}


				
				sb.append(")");
				
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] makeOraCtlFile = " + sb.toString());
				}
				
				
				// ctr 파일 생성(write)
				FileWriter fw = null;
				try {

					fw = new FileWriter(ctlName, false);
					fw.write(sb.toString());
					fw.close();
					
				} catch (Exception ee) {
					XLException.outputExceptionLog(ee);
				} finally {
					try { if ( fw != null ) fw.close(); } catch (Exception e) {} finally { fw = null; }
				}
				
				
				return ctlName;		
				
			} else {
				
				return null;
				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	// cksohn - BULK mode oracle sqlldr
	public static String chkCsvStrFormat_ORACLE(String _data)
    {
           try {
                   
                   if ( _data != null  ) {
                          
                          String data = _data;
                          
                          data = data.replaceAll("\"", "\"\"");

                          return data;
                   } else {
                          
                          // TODO : 타겟이 Oracle 인경우 NULL 처리  
                          //             DATA가 NULL 인 경우, ""으로 저장되어져야 함

                          return "\"\"";
                   }
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl t2t 지원
	// gssg - t2t bulk mode 지원
	public static String makeCtlFile_TIBERO(XLJobRunPol _jopRunPol, String _tableName)
	{
		try {
			
			// 일단은 linux 기준으로. TODO: Windows
			// 1. 파일네임 생성 및 파일 존재 여부 check
			// String pipeName = makePipeName(_dirPath, _srcOrTar);
			String ctlName = _jopRunPol.getPolName()+ "_" + _tableName + ".ctl";
			
			if ( ctlName != null ) {
				
				File file = new File(XLInit.XL_DIR + File.separator + "pipe" + File.separator + ctlName);
				if ( file.exists() ) {
					// 존재하면 삭제
					// XLFileInitManager.outputInfoLog("CKSOHN DEBUG:::: DELETE PIPE : " + file.getAbsolutePath());
					file.delete();
				}
				
				// ~/pipe/ directory 없으면 mkdir
				String pDir = file.getParent();
				File pDirF = new File(pDir);
				if ( !pDirF.exists() ) {
					pDirF.mkdirs();
				}
				
				// full path set
				ctlName = file.getAbsolutePath();
				
				XLJobTableInfo jobTableInfo = _jopRunPol.getTableInfo();
				// 파일 생성
				StringBuffer sb = new StringBuffer();
				
				
				sb.append("LOAD DATA CHARACTERSET UTF8\n")
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' \"STR '|'\"\n")
				
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("'\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
//					.append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' ").append(XLConf.XL_BULK_ORACLE_EOL_CTL_FORMAT).append("\n")
				
				// gssg - xl t2t 지원
				// gssg - t2t bulk mode 지원
				.append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' ").append("\n")
					
				.append("APPEND\n");
					// .append("TRUNCATE INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					
					
					// .append("APPEND INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
				
					if(_jopRunPol.getTdbInfo().getDbType() == XLCons.ORACLE)
					{
						sb.append("INTO TABLE ").append((jobTableInfo.getTowner()).toUpperCase() + "." + (jobTableInfo.getTtable()).toUpperCase() + "\n");
					}
					else
					{
						sb.append("INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n");
					}
					
//					.append("APPEND\n")
					
					
					//.append("INSERT INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					// gssg - o2m  하다가 t2t bulk mode 보완
					sb.append("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\'\n")
					.append("TRAILING NULLCOLS\n")
					.append("(");
				
				// 컬럼 정보 settting
				Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();
				for ( int i=0; i<vtJobColInfo.size(); i++ ) {
					
					if ( i != 0 ) {
						sb.append(",\n");
					}
					
					XLJobColInfo colInfo = vtJobColInfo.get(i);
					if(_jopRunPol.getTdbInfo().getDbType() == XLCons.ORACLE)
					{
						sb.append("\"" + (colInfo.getColName_map().toUpperCase()) + "\""); // 컬럼명
					}
					else
					{
						sb.append("\"" + colInfo.getColName_map() + "\""); // 컬럼명
					}
				
					
					switch ( colInfo.getDataType()) {
					
						case XLDicInfoCons.CHAR:
						case XLDicInfoCons.NCHAR:
						case XLDicInfoCons.VARCHAR:
						case XLDicInfoCons.VARCHAR2:						
							// gssg - xl function 기능 지원
							// gssg - tibero bulk mode function 지원
//							if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
//								sb.append(" CHAR(4000) " + "\"" + colInfo.getFunctionStr() + "(:" + 
//							colInfo.getColName_map() + ")\"");
//							} else {
//								sb.append(" CHAR(4000)");
//							}							
							
							sb.append(" CHAR(4000)");
							break;
						
						// gssg - xl t2t 지원
						// gssg - t2t bulk mode lob 타입 지원
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.NCLOB:
						case XLDicInfoCons.LONG:
							sb.append(" CHAR(4000)");
							break;
							
						case XLDicInfoCons.DATE:							
							// gssg - xl p2t 지원
							// gssg - p2t bulk mode 타입 처리
							// gssg - xl 전체적으로 보완2
							// gssg - PostgreSQL 커넥터 처리
							if ( _jopRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							_jopRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
								// 딕셔너리 구성시 TIMESTAMP 가 DATE 로 등록됨 => PPAS 에서는 TIMESTAMP 와 DATE 타입이 동일
								sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");
							} else {
								sb.append(" DATE \"YYYY-MM-DD HH24:MI:SS\"");								
							}							
							break;
						
						// gssg - xl p2t 지원
						// gssg - p2t 딕셔너리 정보 수정
						case XLDicInfoCons.TIMESTAMP:
						case XLDicInfoCons.TIMESTAMP_LTZ:
						case XLDicInfoCons.TIMESTAMP_TZ:						
							sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");
							break;

						// gssg - xl t2t 지원
						// gssg - t2t bulk mode lob 타입 지원							
						// 티베로 바이너리 타입 지원 x
						case XLDicInfoCons.RAW:
						case XLDicInfoCons.BLOB:
						case XLDicInfoCons.LONGRAW:
							sb.append(" RAW(4000)");
							break;							
							
					}
					
					// gssg - xl function 기능 지원
					// gssg - tibero bulk mode function 지원
					if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
						
						if(_jopRunPol.getTdbInfo().getDbType() == XLCons.ORACLE)
						{
							sb.append(" \"" + colInfo.getFunctionStr() + "(:" + (colInfo.getColName_map()).toUpperCase() + ")\"");
						}
						else
						{
							sb.append(" \"" + colInfo.getFunctionStr() + "(:" + colInfo.getColName_map() + ")\"");
						}
						
					}					
				} // for-end
									
				sb.append(")");
				
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] makeTiberoCtlFile = " + sb.toString());
				}
				
				
				// ctr 파일 생성(write)
				FileWriter fw = null;
				try {

					fw = new FileWriter(ctlName, false);
					fw.write(sb.toString());
					fw.close();
					
				} catch (Exception ee) {
					XLException.outputExceptionLog(ee);
				} finally {
					try { if ( fw != null ) fw.close(); } catch (Exception e) {} finally { fw = null; }
				}
				
				
				return ctlName;		
				
			} else {
				
				return null;
				
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	// cksohn - BULK mode oracle sqlldr
	public static String replaceCharToCSV_ORACLE(String _data)
    {
           try {
                   
               if ( _data != null  ) {
                      
                      // String data = _data;
                      
                      String data = _data.replaceAll("\"", "\"\"");

                      return data;
                      
               } else {
            	   
            	   return "";
               }
               
               
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - o2m  하다가 t2t bulk mode 보완
	public static String replaceCharToCSV_TIBERO(String _data)
    {
           try {
                   
               if ( _data != null  ) {
                      
            	   String data = _data.replace("\\", "\\\\");		// 역슬래시에 대해 하나 더 붙여줌 -> escaped 문자 처리
            	   data = data.replace("\"", "\\\"");			//	더블쿼터에 대해 역슬래시를 붙여줌 -> enclosed 문자 처리
                   return data;
                      
               } else {
            	   
            	   return "";
               }
               
               
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl m2m bulk mode 지원
	public static String replaceCharToCSV_MYSQL(String _data)
    {
           try {
    		   // gssg - 전체적으로 보완_start_20221101
    		   // gssg - m2m bulk mode 역슬래시 데이터 처리
        	   if ( _data != null) {
        		   String data = "";

               	   if ( _data.equals("\\") ) {
                   	   data = _data.replace("\\", "\\\\");	// MySQL 에서 역슬래시(\) 값에 대한 에러 처리            		   
               	   } else {
            		   data = _data.replaceAll("\"", "\"\"");
               	   }                  	   

               	   return data;
               	   
               } else {            	   
            	   return "\\N";
               }
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }           
    }

	// gssg - xl p2p 지원
	// gssg - p2p bulk mode 타입 처리
	public static String replaceCharToCSV_PPAS(String _data)
    {
           try {
                   
        	   // gssg - 전체적으로 보완_start_20221101
        	   // gssg - potgresql to postgresql bulk mode 역슬래시 처리        	   
        	   // gssg - 모듈 보완
        	   // gssg - P2P - \N값 처리
//               if ( _data != null ) {
            	   
            	   String data = null;
            	   
            	   if (_data != null ) {
                	   if ( _data.equals("\\") ) {
                    	   data = _data.replace("\\", "\\\\");		// PostgreSQL 에서 역슬래시(\) 값에 대한 에러 처리            		   
                	   } else {
                    	   data = _data.replace("\"", "\\\""); // gssg - ppas 에서 " 를 복제하려면 앞에 \를 붙여줘야 함            		   
                	   }            		   
            	   }            	   
            	   	return data;
                      
//               } else {   
//            	   
//            	   return "\\N";            	   
//               }                              
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl o2m 지원
	public static String replaceCharToCSV_OracleToMySQL(String _data)
    {
           try {
                   
               if ( _data != null && !_data.equals("") ) {
                      
                      // String data = _data;                      
                      String data = _data.replaceAll("\"", "\"\"");
                      // gssg - xl o2m 지원
                      data = data.replace("\\", "\\\\"); 	// 오라클에서 \n, \t 와 같은 값이 문자 그대로 들어감

                      return data;
                      
               } else {
            	   
            	   return "\\N";
               }                              
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl o2p bulk mode 지원
	// gssg - ppas bulk mode null 처리
	public static String replaceCharToCSV_OracleToPPAS(String _data)
    {
           try {

				// gssg - 삼성물산 - \N값 처리
//               if ( _data != null && !_data.equals("") ) {

            	   String data = null;
            	   // gssg - xl function 기능 지원
            	   // gssg - t2p bulk mode 역슬래시 데이터 처리
                   if ( _data != null  ) {
                	   if ( _data.equals("\\") ) {
                    	   data = _data.replace("\\", "\\\\");		// PostgreSQL 에서 역슬래시(\) 값에 대한 에러 처리            		   
                	   } else {
                    	   data = _data.replace("\"", "\\\""); // gssg - ppas 에서 " 를 복제하려면 앞에 \를 붙여줘야 함            		   
                	   }
                   }
            	   	
                   return data;
                      
//               } else {   
            	   
//            	   return "\\N";            	   
//               }                              
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }




	
}
