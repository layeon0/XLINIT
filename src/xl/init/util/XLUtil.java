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
		// equals --> equalsIgnoreCase �� ����
		if(_type.equalsIgnoreCase("ORACLE")){
			return XLCons.ORACLE;
		} else if ( _type.equalsIgnoreCase("INFORMIX") ) { // // cksohn 3.5.00-001 2015-02-06 - Informix to Informix - Informix Catalog DB
			return XLCons.INFORMIX;
		}else if(_type.equalsIgnoreCase("MYSQL")){
			return XLCons.MYSQL;
		}else if(_type.equalsIgnoreCase("MARIADB")){ // gssg - xl MariaDB ����ȭ
			return XLCons.MARIADB;
		}else if(_type.equalsIgnoreCase("ALTIBASE")){ // cksohn - for Altibase porting
			return XLCons.ALTIBASE;
//		}else if(_type.equalsIgnoreCase("SUNDB")){ // cksohn - for sundb
//			return XLCons.SUNDB;			
			// cksohn SUNDB-->GOLDILOCKS
		}else if(_type.equalsIgnoreCase("ALTIBASE5")){ // gssg - xl MariaDB ����ȭ
			// gssg - ���������ڿ������� �������̰� ���
			// gssg - Altibase5 to Altibase7 ����
			return XLCons.ALTIBASE5;
		}else if(_type.equalsIgnoreCase("GOLDILOCKS")){ // cksohn - for sundb
			return XLCons.GOLDILOCKS;
		} else if (_type.equalsIgnoreCase("PPAS") ) { // cksohn - for otop
			return XLCons.PPAS;
		} else if (_type.equalsIgnoreCase("POSTGRESQL") ) {
			// gssg - xl ��ü������ ����2
			// gssg - PostgreSQL Ŀ���� ó��
			return XLCons.POSTGRESQL;
		} else if (_type.equalsIgnoreCase("TIBERO")){ // cksohn - for otot
			return XLCons.TIBERO;			
		} else if (_type.equalsIgnoreCase("MSSQL")){ // cksohn - for otot
			// gssg - ms2ms ����
			// gssg - MSSQL DBMS �߰�
			return XLCons.MSSQL;
		} else if (_type.equalsIgnoreCase("CUBRID")){
			// gssg - ���������ڿ������� �������̰� ���
			// gssg - Cubrid to Cubrid ����
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
	
	
	// gssg - xl p2t ����
	// gssg - p2t �ϴٰ� o2m time zone ó��
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
	
	
	// gssg - xl p2t ����
	// gssg - p2t �ϴٰ� o2m time zone ó��
	public static String removeMySQLTZ(String _tzValue) {
		
		// TZ : 2022-09-27 16:41:52.868549 +9:00		->			2022-09-27 16:41:52.868549
		// LTZ : 2022-09-27 16:41:51.447679 Asia/Seoul		->			2022-09-27 16:41:51.447679
		
			try {
				if ( _tzValue != null ) {
					_tzValue = _tzValue.substring(0, _tzValue.lastIndexOf(" "));					
					return _tzValue;
				} else {
					return "\\N"; // mysql�� \N �� null �� ��
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				return _tzValue;
			}	
		}

	
	// gssg - ���������ڿ������� �������̰� ���
	// gssg - Oracle to Oracle Ÿ���� ó��
	public static String removeOracleTZ(String _tzValue) {
		
		// TZ : 2022-09-27 16:41:52.868549 +9:00		->			2022-09-27 16:41:52.868549
		// LTZ : 2022-09-27 16:41:51.447679 Asia/Seoul		->			2022-09-27 16:41:51.447679
		
			try {
				if ( _tzValue != null ) {
					_tzValue = _tzValue.substring(0, _tzValue.lastIndexOf(" "));					
					return _tzValue;
				} else {
					return ""; // oracle�� "" �� null �� ��
				}
				
			} catch (Exception e) {
				e.printStackTrace();
				return _tzValue;
			}	
		}

	
	
	// cksohn - SEQ ���� �Ϲ���å���� �����ϵ��� ����
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
	

	// cksohn - DDL "" ó�� �̽�
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
	
	// cksohn - kill -9 ���� ����  - to disk log
	// file�� line �� return
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

	    // cksohn - hexToBytesArray() ���� ���� start - [
	    try {
	    	return Hex.decodeHex(hex.toCharArray());	    	
	    } catch (Exception e) {
	    	e.printStackTrace();
	    	return null;
	    }
	    // ] - end // cksohn - hexToBytesArray() ���� ����
	}
	
	public static String bytesToHexString(byte[] bytes) {
		
		try {
			
			return Hex.encodeHexString(bytes);
			
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	} 
	
	
	// ���� ��¥ --> String format yyyy-MM-dd HH:mm:ss
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
			
			// �ϴ��� linux ��������. TODO: Windows
			// 1. ���ϳ��� ���� �� ���� ���� ���� check
			// String pipeName = makePipeName(_dirPath, _srcOrTar);
			String pipeName = _polName + "_" + _tableName + "_pipe";
			
			if ( pipeName != null ) {
				
				File file = new File(XLInit.XL_DIR + File.separator + "pipe" + File.separator + pipeName);
				if ( file.exists() ) {
					// �����ϸ� ����
					// XLFileInitManager.outputInfoLog("CKSOHN DEBUG:::: DELETE PIPE : " + file.getAbsolutePath());
					file.delete();
				}
				
				// ~/pipe/ directory ������ mkdir
				String pDir = file.getParent();
				File pDirF = new File(pDir);
				if ( !pDirF.exists() ) {
					pDirF.mkdirs();
				}
				
				// ���� ���� 
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
	
	// gssg - csv file create ��� �߰�
	// gssg - csv ���� ����
	public static String makeCSV(XLJobRunPol _jopRunPol)
	{
		try {
			
			XLJobTableInfo jobTableInfo = _jopRunPol.getTableInfo();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			Date now = new Date();
			String nowTime = sdf.format(now);
			
			// gssg - csv file create ��� �߰�
			// gssg - file ������ �Ϸ�Ǹ� Ȯ���� ����
			String csvName = jobTableInfo.getSowner() + "-" + jobTableInfo.getStable() + "-" + nowTime + ".tmp";
			
			if ( csvName != null ) {
				
				File file = new File(XLConf.XL_CREATE_FILE_PATH + File.separator + csvName);
				
				// directory ������ mkdir
				String cDir = file.getParent();
				File cDirF = new File(cDir);
				if ( !cDirF.exists() ) {
					cDirF.mkdirs();
				}

				// ���� ���� 
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
	
	// gssg - csv file create ��� �߰�
	// gssg - file ������ �Ϸ�Ǹ� Ȯ���� ����
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
	 * [ �Ϲ� Ÿ�� sample ]
	 * LOAD DATA CHARACTERSET UTF8 
			INFILE '/home/XLOG/OtoO_TTA/XLManager/csval/PTA101/TESTUSER.XL_VER_TEST01' "STR '|'"
			TRUNCATE INTO TABLE TESTUSER.XL_VER_TEST01
			FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' 
			TRAILING NULLCOLS
			("ID", 
			"NAME" CHAR(4000), 
			"ADDR" CHAR(4000), 
			"REGDATE" TIMESTAMP "YYYY-MM-DD HH24:MI:SS.FF")
			
	   [ LOB �÷� sample ]
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
			
			// �ϴ��� linux ��������. TODO: Windows
			// 1. ���ϳ��� ���� �� ���� ���� ���� check
			// String pipeName = makePipeName(_dirPath, _srcOrTar);
			String ctlName = _jobRunPol.getPolName() + "_" + _tableName + ".ctl";
			
			if ( ctlName != null ) {
				
				File file = new File(XLInit.XL_DIR + File.separator + "pipe" + File.separator + ctlName);
				if ( file.exists() ) {
					// �����ϸ� ����
					// XLFileInitManager.outputInfoLog("CKSOHN DEBUG:::: DELETE PIPE : " + file.getAbsolutePath());
					file.delete();
				}
				
				// ~/pipe/ directory ������ mkdir
				String pDir = file.getParent();
				File pDirF = new File(pDir);
				if ( !pDirF.exists() ) {
					pDirF.mkdirs();
				}
				
				// full path set
				ctlName = file.getAbsolutePath();
				
				XLJobTableInfo jobTableInfo = _jobRunPol.getTableInfo();
				// ���� ����
				StringBuffer sb = new StringBuffer();
				
				
				sb.append("LOAD DATA CHARACTERSET UTF8\n")
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' \"STR '|'\"\n")
				
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("'\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
					.append("INFILE '").append(_jobRunPol.getBulk_pipePath()).append("' ").append(XLConf.XL_BULK_ORACLE_EOL_CTL_FORMAT).append("\n")
					
					
					
					// .append("TRUNCATE INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					
					
					// .append("APPEND INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
					// gssg - ���������ڿ������� �������̰� ���
					// gssg - Altibase to Oracle ����
//					.append("INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					.append("INTO TABLE \"").append((jobTableInfo.getTowner()).toUpperCase() + "\".\"" + (jobTableInfo.getTtable()).toUpperCase() + "\"\n")
					.append("APPEND\n")
					
					
					//.append("INSERT INTO TABLE ").append(jobTableInfo.getTowner() + "." + jobTableInfo.getTtable() + "\n")
					.append("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n ")
					.append("TRAILING NULLCOLS\n")
					.append("(");
				
				// �÷� ���� settting
				Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();
				for ( int i=0; i<vtJobColInfo.size(); i++ ) {
					
					if ( i != 0 ) {
						sb.append(",\n");
					}
					
					XLJobColInfo colInfo = vtJobColInfo.get(i);
					
					sb.append("\"" + (colInfo.getColName_map()).toUpperCase() + "\""); // �÷���
					
					switch ( colInfo.getDataType()) {
					
						case XLDicInfoCons.CHAR:
						case XLDicInfoCons.NCHAR:
						case XLDicInfoCons.VARCHAR:
						case XLDicInfoCons.VARCHAR2:
							// gssg - ����� O2O
							// gssg - raw_to_varchar2 ��� ����												
//							sb.append(" CHAR(4000)");
							sb.append(" CHAR(8000)");
							break;

						// gssg - LG���� MS2O
						// gssg - ms2o bulk mode ����
						case XLDicInfoCons.SMALLDATETIME:	
						case XLDicInfoCons.DATE:
							// gssg - ���������ڿ������� �������̰� ���
							// gssg - Altibase to Oracle ����
							// gssg - Altibase5 to Altibase7 ����
							if ( _jobRunPol.getSdbInfo().getDbType() == XLCons.ALTIBASE || _jobRunPol.getSdbInfo().getDbType() == XLCons.ALTIBASE5 ) {
								sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");								
							} else {
								sb.append(" DATE \"YYYY-MM-DD HH24:MI:SS\"");								
							}
							
							break;
							
						// gssg - LG���� MS2O
						// gssg - ms2o bulk mode ����
						case XLDicInfoCons.DATETIME:
						case XLDicInfoCons.DATETIME2:							
						case XLDicInfoCons.TIMESTAMP:
							sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");
							break;
						
						// gssg - ���������ڿ������� �������̰� ���
						// gssg - Oracle to Oracle Ÿ���� ó��
						// gssg - LG���� MS2O
						// gssg - ms2o bulk mode ����
						// gssg - o2o bulk tz ����
						case XLDicInfoCons.DATETIMEOFFSET:
						case XLDicInfoCons.TIMESTAMP_TZ:
							// "T" "TO_TIMESTAMP_TZ(:T,'YYYY-MM-DD HH24:MI:SS:FF TZHTZM')")
//							sb.append(" \"TO_TIMESTAMP_TZ(:" + colInfo.getColName_map() + ", 'YYYY-MM-DD HH24:MI:SS.FF TZHTZM')\"");
							sb.append(" \"TO_TIMESTAMP_TZ(:" + colInfo.getColName_map() + ", 'YYYY-MM-DD HH24:MI:SS.FF TZR')\"");

							break;
						
						// gssg - LG���� MS2O
						// gssg - o2o bulk ltz ����
						case XLDicInfoCons.TIMESTAMP_LTZ:
							sb.append(" \"FROM_TZ(TO_TIMESTAMP(:" + colInfo.getColName_map() + ", 'YYYY-MM-DD HH24:MI:SS.FF'), '" + XLConf.XL_TIMEZONE + "')\"");
							break;
						
							
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.BLOB:
							sb.append(" CHAR(1000000)NULLIF (\"" + colInfo.getColName_map() + "\"=X'FF')");
							break;							
							
					}
										
					// gssg - xl function ��� ����
					// gssg - oracle bulk mode function ����
					if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
						sb.append(" \"" + colInfo.getFunctionStr() + "(:" + (colInfo.getColName_map()).toUpperCase() + ")\"");
					}					
				} // for-end
				
				// gssg - LG���� MS2O
				// gssg - �����ڵ� �� ó��
				if ( _jobRunPol.getCustomValue() != null && !_jobRunPol.getCustomValue().equals("") ) {
					sb.append(",\n\"" + _jobRunPol.getCustomColname() + "\" CONSTANT \"" + _jobRunPol.getCustomValue() + "\"");										
				}


				
				sb.append(")");
				
				if ( XLConf.XL_DEBUG_YN ) {
					XLLogger.outputInfoLog("[DEBUG] makeOraCtlFile = " + sb.toString());
				}
				
				
				// ctr ���� ����(write)
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
                          
                          // TODO : Ÿ���� Oracle �ΰ�� NULL ó��  
                          //             DATA�� NULL �� ���, ""���� ����Ǿ����� ��

                          return "\"\"";
                   }
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl t2t ����
	// gssg - t2t bulk mode ����
	public static String makeCtlFile_TIBERO(XLJobRunPol _jopRunPol, String _tableName)
	{
		try {
			
			// �ϴ��� linux ��������. TODO: Windows
			// 1. ���ϳ��� ���� �� ���� ���� ���� check
			// String pipeName = makePipeName(_dirPath, _srcOrTar);
			String ctlName = _jopRunPol.getPolName()+ "_" + _tableName + ".ctl";
			
			if ( ctlName != null ) {
				
				File file = new File(XLInit.XL_DIR + File.separator + "pipe" + File.separator + ctlName);
				if ( file.exists() ) {
					// �����ϸ� ����
					// XLFileInitManager.outputInfoLog("CKSOHN DEBUG:::: DELETE PIPE : " + file.getAbsolutePath());
					file.delete();
				}
				
				// ~/pipe/ directory ������ mkdir
				String pDir = file.getParent();
				File pDirF = new File(pDir);
				if ( !pDirF.exists() ) {
					pDirF.mkdirs();
				}
				
				// full path set
				ctlName = file.getAbsolutePath();
				
				XLJobTableInfo jobTableInfo = _jopRunPol.getTableInfo();
				// ���� ����
				StringBuffer sb = new StringBuffer();
				
				
				sb.append("LOAD DATA CHARACTERSET UTF8\n")
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' \"STR '|'\"\n")
				
					// .append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("'\n")
					// cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL
//					.append("INFILE '").append(_jopRunPol.getBulk_pipePath()).append("' ").append(XLConf.XL_BULK_ORACLE_EOL_CTL_FORMAT).append("\n")
				
				// gssg - xl t2t ����
				// gssg - t2t bulk mode ����
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
					// gssg - o2m  �ϴٰ� t2t bulk mode ����
					sb.append("FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\'\n")
					.append("TRAILING NULLCOLS\n")
					.append("(");
				
				// �÷� ���� settting
				Vector<XLJobColInfo> vtJobColInfo = jobTableInfo.getVtColInfo();
				for ( int i=0; i<vtJobColInfo.size(); i++ ) {
					
					if ( i != 0 ) {
						sb.append(",\n");
					}
					
					XLJobColInfo colInfo = vtJobColInfo.get(i);
					if(_jopRunPol.getTdbInfo().getDbType() == XLCons.ORACLE)
					{
						sb.append("\"" + (colInfo.getColName_map().toUpperCase()) + "\""); // �÷���
					}
					else
					{
						sb.append("\"" + colInfo.getColName_map() + "\""); // �÷���
					}
				
					
					switch ( colInfo.getDataType()) {
					
						case XLDicInfoCons.CHAR:
						case XLDicInfoCons.NCHAR:
						case XLDicInfoCons.VARCHAR:
						case XLDicInfoCons.VARCHAR2:						
							// gssg - xl function ��� ����
							// gssg - tibero bulk mode function ����
//							if ( colInfo.getFunctionStr() != null && !colInfo.getFunctionStr().equals("") ) {
//								sb.append(" CHAR(4000) " + "\"" + colInfo.getFunctionStr() + "(:" + 
//							colInfo.getColName_map() + ")\"");
//							} else {
//								sb.append(" CHAR(4000)");
//							}							
							
							sb.append(" CHAR(4000)");
							break;
						
						// gssg - xl t2t ����
						// gssg - t2t bulk mode lob Ÿ�� ����
						case XLDicInfoCons.CLOB:
						case XLDicInfoCons.NCLOB:
						case XLDicInfoCons.LONG:
							sb.append(" CHAR(4000)");
							break;
							
						case XLDicInfoCons.DATE:							
							// gssg - xl p2t ����
							// gssg - p2t bulk mode Ÿ�� ó��
							// gssg - xl ��ü������ ����2
							// gssg - PostgreSQL Ŀ���� ó��
							if ( _jopRunPol.getSdbInfo().getDbType() == XLCons.PPAS || 
							_jopRunPol.getSdbInfo().getDbType() == XLCons.POSTGRESQL ) {
								// ��ųʸ� ������ TIMESTAMP �� DATE �� ��ϵ� => PPAS ������ TIMESTAMP �� DATE Ÿ���� ����
								sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");
							} else {
								sb.append(" DATE \"YYYY-MM-DD HH24:MI:SS\"");								
							}							
							break;
						
						// gssg - xl p2t ����
						// gssg - p2t ��ųʸ� ���� ����
						case XLDicInfoCons.TIMESTAMP:
						case XLDicInfoCons.TIMESTAMP_LTZ:
						case XLDicInfoCons.TIMESTAMP_TZ:						
							sb.append(" TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"");
							break;

						// gssg - xl t2t ����
						// gssg - t2t bulk mode lob Ÿ�� ����							
						// Ƽ���� ���̳ʸ� Ÿ�� ���� x
						case XLDicInfoCons.RAW:
						case XLDicInfoCons.BLOB:
						case XLDicInfoCons.LONGRAW:
							sb.append(" RAW(4000)");
							break;							
							
					}
					
					// gssg - xl function ��� ����
					// gssg - tibero bulk mode function ����
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
				
				
				// ctr ���� ����(write)
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
	
	// gssg - o2m  �ϴٰ� t2t bulk mode ����
	public static String replaceCharToCSV_TIBERO(String _data)
    {
           try {
                   
               if ( _data != null  ) {
                      
            	   String data = _data.replace("\\", "\\\\");		// �������ÿ� ���� �ϳ� �� �ٿ��� -> escaped ���� ó��
            	   data = data.replace("\"", "\\\"");			//	�������Ϳ� ���� �������ø� �ٿ��� -> enclosed ���� ó��
                   return data;
                      
               } else {
            	   
            	   return "";
               }
               
               
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl m2m bulk mode ����
	public static String replaceCharToCSV_MYSQL(String _data)
    {
           try {
    		   // gssg - ��ü������ ����_start_20221101
    		   // gssg - m2m bulk mode �������� ������ ó��
        	   if ( _data != null) {
        		   String data = "";

               	   if ( _data.equals("\\") ) {
                   	   data = _data.replace("\\", "\\\\");	// MySQL ���� ��������(\) ���� ���� ���� ó��            		   
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

	// gssg - xl p2p ����
	// gssg - p2p bulk mode Ÿ�� ó��
	public static String replaceCharToCSV_PPAS(String _data)
    {
           try {
                   
        	   // gssg - ��ü������ ����_start_20221101
        	   // gssg - potgresql to postgresql bulk mode �������� ó��        	   
        	   // gssg - ��� ����
        	   // gssg - P2P - \N�� ó��
//               if ( _data != null ) {
            	   
            	   String data = null;
            	   
            	   if (_data != null ) {
                	   if ( _data.equals("\\") ) {
                    	   data = _data.replace("\\", "\\\\");		// PostgreSQL ���� ��������(\) ���� ���� ���� ó��            		   
                	   } else {
                    	   data = _data.replace("\"", "\\\""); // gssg - ppas ���� " �� �����Ϸ��� �տ� \�� �ٿ���� ��            		   
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
	
	// gssg - xl o2m ����
	public static String replaceCharToCSV_OracleToMySQL(String _data)
    {
           try {
                   
               if ( _data != null && !_data.equals("") ) {
                      
                      // String data = _data;                      
                      String data = _data.replaceAll("\"", "\"\"");
                      // gssg - xl o2m ����
                      data = data.replace("\\", "\\\\"); 	// ����Ŭ���� \n, \t �� ���� ���� ���� �״�� ��

                      return data;
                      
               } else {
            	   
            	   return "\\N";
               }                              
                   
           } catch (Exception e) {
                   e.printStackTrace();
                   return _data;
           }
           
    }
	
	// gssg - xl o2p bulk mode ����
	// gssg - ppas bulk mode null ó��
	public static String replaceCharToCSV_OracleToPPAS(String _data)
    {
           try {

				// gssg - �Ｚ���� - \N�� ó��
//               if ( _data != null && !_data.equals("") ) {

            	   String data = null;
            	   // gssg - xl function ��� ����
            	   // gssg - t2p bulk mode �������� ������ ó��
                   if ( _data != null  ) {
                	   if ( _data.equals("\\") ) {
                    	   data = _data.replace("\\", "\\\\");		// PostgreSQL ���� ��������(\) ���� ���� ���� ó��            		   
                	   } else {
                    	   data = _data.replace("\"", "\\\""); // gssg - ppas ���� " �� �����Ϸ��� �տ� \�� �ٿ���� ��            		   
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
