package xl.init.dbmgr;

import java.util.Hashtable;

public class XLDBDataTypes {
	public static final int DIC = 0;
	public static final int NODIC = 1;
	
	public static final int INSERT = 1;
	public static final int DELETE = 2;	
	public static final int UPDATE = 3;
	public static final int INSERT_LONGDATA = 101;
	public static final int UPDATE_LONGDATA = 103;
	public static final String SELECTCOUNT_INDAY = "SD"; 
	public static final String SELECTCOUNT_INORG = "SO";
	public static final String SELECT_INSERT = "SI";
	
	// cksohn - LOB like type 복제 - porting cksohn - IFX LOB type APPLY performance start - [ 
	// public static final int SELECT_IFXLOB = 10;
	public static final int SELECT_ORALOB = 10;
	// public static final int SELECT_ORALOB_BY_ROWID = 11;
	public static final int SELECT_ORALOBPK_BY_ROWID = 11;
	
	public static final int SELECT_ORALONG = 12; // LONG/LONGRAW
	
	// ] - end cksohn - LOB like type 복제 
	
	
//	public static final String COLFORMAT1 = ",";
//	public static final String COLFORMAT2 = "(";
//	public static final String COLFORMAT3 = ")";
//	public static final String COLFORMAT4 = " = "; 
//	public static final String COLFORMAT5 = " AND ";
//	public static final String COLFORMAT6 = " OR ";
//	public static final String COLFORMAT7 = " NULL ";
//	public static final String COLFORMAT8 = " ? ";
//	public static final String COLFORMAT9 = " INSERT INTO ";
//	public static final String COLFORMAT10 = " VALUES ";
//	public static final String COLFORMAT11 = " UPDATE "; 
//	public static final String COLFORMAT12 = " SET ";
//	public static final String COLFORMAT13 = " DELETE FROM "; 
//	public static final String COLFORMAT14 = " WHERE ";	
//	public static final String COLFORMAT15 = " HEXTORAW ";
//	public static final String COLFORMAT16 = " = ";
//	public static final String COLFORMAT17 = " IS ";
//	public static final String COLFORMAT18 = " AND ROWNUM=1 ";
	
	
	/**일 변동 관련 String**/
//	public static final String COLFORMAT100 = " IUD_GB ";  //일변동 COLUMN(I/D/U)
//	public static final String COLFORMAT101 = " EDW_DATE "; //일변동 COLUMN(YYYY/MM/DD)
//	public static final String COLFORMAT102 = " SELECT ";
//	public static final String COLFORMAT103 = " COUNT(*) CNT ";
//	public static final String COLFORMAT104 = " FROM ";
//	public static final String COLFORMAT105 = " * ";
	
	public static final int VARCHAR2 = 0;
	public static final int CHAR = 1;
	public static final int DATE = 2;	
	public static final int NUMBER = 3;
	//3.2.00-006 2011-07-11 modify add data type - start
	public static final int FLOAT = 4;
	public static final int RAW = 5;
	public static final int TIMESTAMP = 6;
	//3.2.00-006 2011-07-11 modify add data type - end
	//3.2.00-002 2010-05-19 modify for big type - start
	public static final int LONG = 10;
	public static final int LONGRAW = 11;
	public static final int CLOB = 12;	
	public static final int NCLOB = 13;	
	public static final int BLOB = 14;
	//3.2.00-002 2010-05-19 modify for big type - end
	//sykim 3.2.01-013 2013-03-07 for add timestamp_ltz
	public static final int TIMESTAMP_LTZ = 7;
	
	// cksohn 3.5.00-001 add NVARCHAR2 type 
	public static final int NVARCHAR2 = 15;      
	
	// cksohn - NCHAR 추가 
	public static final int NCHAR = 16;
 
	// cksohn - ROWID 복제 기능 추가
	public static final int ROWID = 17;
	
	//cksohn - BINARY_FLOAT/BINARY_DOUBLE 복제 기능 추가 start - [
	public static final int BINARY_FLOAT = 18;
	public static final int BINARY_DOUBLE = 19;
	// ] - end cksohn - BINARY_FLOAT/BINARY_DOUBLE 복제 기능 추가
	
	// cksohn - LOB like type 복제 start - [
	public static final int BFILE = 20;
	public static final int XMLTYPE = 21;
	// ] - end cksohn - LOB like type 복제
	
	// cksohn - INTERVAL type 복제 기능
	public static final int INTERVAL_DS= 22; // DAY TO SECOND
	public static final int INTERVAL_YM= 23; // YEAR TO MONTH
	
	// cksohn - for otop - 데이터 타입 설정 부분 PPAS로 부터 포팅
	// cksohn - for mssql replication - MSSQL DataTypes start - [
	// MSSQL 에만 존재하는 추가된 데이터 타입
	
	public static final int BINARY 		= 30;
	public static final int BIT			= 31;
	public static final int DATETIME	= 32;
	public static final int DATETIME2	= 33;
	public static final int DATETIMEOFFSET	= 34;
	public static final int DECIMAL		= 35;
	public static final int GEOGRAPHY	= 36;
	public static final int GEOMETRY	= 37;
	public static final int HIERARCHYID	= 38;
	public static final int IMAGE		= 39;
	public static final int MONEY		= 40;
	public static final int TEXT		= 41;
	public static final int NTEXT		= 42;
	public static final int NUMERIC		= 43;
	public static final int VARCHAR		= 44;
	public static final int NVARCHAR	= 45;
	public static final int REAL		= 46;
	public static final int SMALLDATETIME	= 47;
	public static final int SMALLMONEY	= 48;
	public static final int SQLVARIANT	= 49;
	public static final int TIME		= 50; // time(7)
	public static final int UNIQUEIDENTIFIER	= 51;
	public static final int INT			= 52;
	public static final int INTEGER			= 52; // cksohn - for ppas - setByType
	
	public static final int VARBINARY		= 53;
	
	public static final int VARCHARMAX	= 60;
	public static final int NVARCHARMAX	= 61;
	
	
	// ] - end cksohn - for mssql replication - MSSQL DataTypes 
	
	// cksohn - for ppas - setByType start - [
	
	public static final int SMALLINT	= 70;
	public static final int BIGINT	 	= 71;
	
	public static final int BYTEA	 	= 72; // cksohn - for ppas sepcial data type
	
	
	// ] - cksohn - for ppas - setByType
	
	
	// cksohn - LOB like type 복제 start - [ 
	// BIGTYPE default value
	public static final String CLOB_DEFAULT = " ";
	public static final String LONG_DEFAULT = " ";
	public static final String BLOB_DEFAULT = "20"; // space hex value
	public static final String LONGRAW_DEFAULT = "20"; // space hex value
	
	// ] - end cksohn - LOB like type 복제
	
	public static final Hashtable<Integer, String> HT_ORA_NODICDATA = new Hashtable<Integer, String>();
    static {
    	HT_ORA_NODICDATA.put(XLDBDataTypes.NUMBER, "RAW_TO_NUM");
    	
    	
    	// cksohn - NEW RAW_TO_VARCHAR2 FUNCTION for 4000 bytes - UTL_RAW.CAST_TO_VARCHAR2
    	HT_ORA_NODICDATA.put(XLDBDataTypes.CHAR, "RAW_TO_VARCHAR2");
    	HT_ORA_NODICDATA.put(XLDBDataTypes.VARCHAR2,  "RAW_TO_VARCHAR2");
    	
    	// 아래걸로 쓰면 concat too long error 발생함.
//    	HT_ORA_NODICDATA.put(NRDicInfoCons.CHAR, "UTL_RAW.CAST_TO_VARCHAR2");
//    	HT_ORA_NODICDATA.put(NRDicInfoCons.VARCHAR2,  "UTL_RAW.CAST_TO_VARCHAR2");

    	
    	HT_ORA_NODICDATA.put(XLDBDataTypes.DATE,  "RAW_TO_DATE");
    	//3.2.00-002 2010-05-19 modify for big type - start
    	HT_ORA_NODICDATA.put(XLDBDataTypes.LONG,  "RAW_TO_VARCHAR2");
    	HT_ORA_NODICDATA.put(XLDBDataTypes.CLOB,  "RAW_TO_VARCHAR2");
    	HT_ORA_NODICDATA.put(XLDBDataTypes.NCLOB,  "RAW_TO_VARCHAR2"); 
    	//3.2.00-002 2010-05-19 modify for big type - end
    	
    	
    	// cksohn - LOB like type 복제 - XMLTYPE 
    	HT_ORA_NODICDATA.put(XLDBDataTypes.XMLTYPE,  "sys.XMLType.createXML");

    	
//    	//cksohn - NVARCHAR2/NCHAR HEXTORAW() 적용. FUNC_ALLPY=Y 일때 
//    	HT_ORA_NODICDATA.put(NRDicInfoCons.NVARCHAR2,  "HEXTORAW");
//    	HT_ORA_NODICDATA.put(NRDicInfoCons.NCHAR,  "HEXTORAW");
    	
    	
    }
}
