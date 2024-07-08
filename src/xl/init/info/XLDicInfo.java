package xl.init.info;

// gssg - xl m2m type 지원

public class XLDicInfo {

	public static int convertDataType(String _type){
	
		if(_type.equals("VARCHAR2")){
			return XLDicInfoCons.VARCHAR2;
		}
		else if(_type.equals("CHAR")){
			return XLDicInfoCons.CHAR;
		}
		else if(_type.equals("NUMBER") || _type.startsWith("NUMBER(")){//3.2.00-006 2011-07-11 modify add data type
			return XLDicInfoCons.NUMBER;
		}
		else if(_type.equals("DATE")){
			return XLDicInfoCons.DATE;
		}
		//3.2.00-002 2010-05-19 modify for big type - start
		else if(_type.equals("LONG")){
			return XLDicInfoCons.LONG;
		}
		else if(_type.equals("LONG RAW")){
			return XLDicInfoCons.LONGRAW;
		}
		else if(_type.equals("CLOB")){
			return XLDicInfoCons.CLOB;
		}
		else if(_type.equals("NCLOB")){
			return XLDicInfoCons.NCLOB;
		}
		else if(_type.equals("BLOB")){
			return XLDicInfoCons.BLOB;
		}
		//3.2.00-006 2011-07-11 modify add data type - start
		else if(_type.equals("FLOAT")){
			return XLDicInfoCons.FLOAT;
		}
		else if(_type.equals("RAW")){
			return XLDicInfoCons.RAW;
		}
		// gssg - xl p2t 지원
		// gssg - p2t 딕셔너리 정보 수정
//		else if(_type.equals("TIMESTAMP")||_type.startsWith("TIMESTAMP(")){
		else if(_type.startsWith("TIMESTAMP")){
			//sykim 3.2.01-013 2013-03-07 for add timestamp_ltz
			if(_type.indexOf("LOCAL") > -1){
				return XLDicInfoCons.TIMESTAMP_LTZ;				
			} else if ( _type.indexOf("WITHOUT") > -1) { // cksohn - xl o2p 기능 추가
				// gssg - xl PPAS/PostgreSQL 분리
				// gssg - TIMESTAMP 컬럼 DIC_INFO 처리
				return XLDicInfoCons.TIMESTAMP;				
			} else if ( _type.indexOf("ZONE") > -1) { // cksohn - xl o2p 기능 추가
				// gssg - xl PPAS/PostgreSQL 분리
				// gssg - TIMESTAMP 컬럼 DIC_INFO 처리
				return XLDicInfoCons.TIMESTAMP_TZ;				
			} else {
				return XLDicInfoCons.TIMESTAMP;
			}		
		}
		else if(_type.equals("NVARCHAR2")){ // cksohn 3.5.00-001 add NVARCHAR2 type
			return XLDicInfoCons.NVARCHAR2;
		}
		else if(_type.equals("NCHAR")){ // cksohn - NCHAR 추가
			return XLDicInfoCons.NCHAR;
		}
		else if(_type.equals("ROWID")){ // cksohn - ROWID 복제 기능 추가
			return XLDicInfoCons.ROWID;
		}
		else if(_type.equals("BINARY_FLOAT")){ // cksohn - BINARY_FLOAT/BINARY_DOUBLE 복제 기능 추가
			return XLDicInfoCons.BINARY_FLOAT;
		}
		else if(_type.equals("BINARY_DOUBLE")){ // cksohn - BINARY_FLOAT/BINARY_DOUBLE 복제 기능 추가
			return XLDicInfoCons.BINARY_DOUBLE;
		}
		else if(_type.equals("BFILE")){ // cksohn - LOB like type 복제
			return XLDicInfoCons.BFILE;
		}
		else if(_type.equals("XMLTYPE")){ // cksohn - LOB like type 복제
			return XLDicInfoCons.XMLTYPE;
		}
		// cksohn - INTERVAL type 복제 기능 start - [
		else if(_type.startsWith("INTERVAL")){ 
			
			if ( _type.contains("DAY") ){
				return XLDicInfoCons.INTERVAL_DS;
			} else {
				return XLDicInfoCons.INTERVAL_YM;
			}
		}
		// ] - end cksohn - INTERVAL type 복제 기능
		
		
		
		// cksohn - for mssql replication - MSSQL DataTypes
		else if(_type.equalsIgnoreCase("BINARY")){ 
			return XLDicInfoCons.BINARY;
		}
		else if(_type.equalsIgnoreCase("BIT")){ 
			return XLDicInfoCons.BIT;
		}
		else if(_type.equalsIgnoreCase("DATETIME")){
			// gssg - LG엔솔 MS2O
			// gssg - ms2o bulk mode 지원
//			return XLDicInfoCons.DATETIME2;
			return XLDicInfoCons.DATETIME;
		}
		else if(_type.equalsIgnoreCase("DATETIME2")){ 
			return XLDicInfoCons.DATETIME2;
		}
		// else if(_type.equalsIgnoreCase("DATETIMEOFFSEET")){ 
		else if(_type.equalsIgnoreCase("DATETIMEOFFSET")){ // cksohn - DATETIMEOFFSET, TIME 오류 수정
			return XLDicInfoCons.DATETIMEOFFSET;
		}
		else if(_type.equalsIgnoreCase("DECIMAL")){ 
			return XLDicInfoCons.DECIMAL;
		}
		else if(_type.equalsIgnoreCase("GEOGRAPHY")){ 
			return XLDicInfoCons.GEOGRAPHY;
		}
		else if(_type.equalsIgnoreCase("GEOMETRY")){ 
			return XLDicInfoCons.GEOMETRY;
		}
		else if(_type.equalsIgnoreCase("HIERARCHYID")){ 
			return XLDicInfoCons.HIERARCHYID;
		}

		else if(_type.equalsIgnoreCase("IMAGE")){ 
			return XLDicInfoCons.IMAGE;
		}

		else if(_type.equalsIgnoreCase("MONEY")){ 
			return XLDicInfoCons.MONEY;
		}
		else if(_type.equalsIgnoreCase("TEXT")){ 
			return XLDicInfoCons.TEXT;
		}
		else if(_type.equalsIgnoreCase("NTEXT")){ 
			return XLDicInfoCons.NTEXT;
		}
		else if(_type.equalsIgnoreCase("NUMERIC")){ 
			return XLDicInfoCons.NUMERIC;
		}
		else if(_type.equalsIgnoreCase("VARCHAR")){ 
			return XLDicInfoCons.VARCHAR;
		}
		else if(_type.equalsIgnoreCase("NVARCHAR")){ 
			return XLDicInfoCons.NVARCHAR;
		}
		else if(_type.equalsIgnoreCase("REAL")){ 
			return XLDicInfoCons.REAL;
		}
		else if(_type.equalsIgnoreCase("SMALLDATETIME")){ 
			return XLDicInfoCons.SMALLDATETIME;
		}
		else if(_type.equalsIgnoreCase("SMALLMONEY")){ 
			return XLDicInfoCons.SMALLMONEY;
		}
		else if(_type.equalsIgnoreCase("SQLVARIANT")){ 
			return XLDicInfoCons.SQLVARIANT;
		}
		
		
		else if(_type.toUpperCase().startsWith("TIME(") || _type.equalsIgnoreCase("TIME")){ // time(7) // cksohn - DATETIMEOFFSET, TIME 오류 수정
			return XLDicInfoCons.TIME;
		}

		// gssg - 전체적으로 보완_start_20221101
		// gssg - potgresql to postgresql time type 처리
		else if(_type.toUpperCase().startsWith("TIME ")) {
			
			if ( _type.indexOf("WITHOUT") > -1 ) {
				return XLDicInfoCons.TIME;
			} else {
				return XLDicInfoCons.TIME_TZ;				
			}
		} 
		
		else if(_type.equalsIgnoreCase("UNIQUEIDENTIFIER")){ 
			return XLDicInfoCons.UNIQUEIDENTIFIER;
		}
		else if(_type.equalsIgnoreCase("INT") || _type.equalsIgnoreCase("BIGINT") || _type.equalsIgnoreCase("SMALLINT") 
				|| _type.equalsIgnoreCase("INTEGER") ){ // cksohn - for ppas - BIGINT, SMALLINT 
			return XLDicInfoCons.INT;
		}
		
		else if(_type.equalsIgnoreCase("VARBINARY")){ 
			return XLDicInfoCons.VARBINARY;
		}
		
		else if(_type.equalsIgnoreCase("VARCHAR(MAX)")){ 
			return XLDicInfoCons.VARCHARMAX;
		}
		else if(_type.equalsIgnoreCase("NVARCHAR(MAX)")){ 
			return XLDicInfoCons.NVARCHARMAX;
		}
		
		// cksohn - for ppas - setByType start - [
		else if(_type.equalsIgnoreCase("SMALLINT")){ 
			return XLDicInfoCons.SMALLINT;
		}
		else if(_type.equalsIgnoreCase("BIGINT")){ 
			return XLDicInfoCons.BIGINT;
		}
		else if(_type.equalsIgnoreCase("BYTEA")){ 
			return XLDicInfoCons.BYTEA;
		}
		// gssg - xl p2p 지원
		// gssg - p2p normal mode 타입 처리
		else if(_type.equalsIgnoreCase("DOUBLE PRECISION")){ 
			return XLDicInfoCons.DOUBLEPRECISION;
		}
		else if(_type.equalsIgnoreCase("BIT VARYING")){ 
			return XLDicInfoCons.BITVARYING;
		}
		else if(_type.equalsIgnoreCase("XML")){ 
			return XLDicInfoCons.XML;
		}

		// ] - end cksohn - for ppas - setByType

		// ] - end cksohn - for mssql replication - MSSQL DataTypes

		// gssg - xl m2m type 지원
		// gssg - for mysql replication - MySQL DataTypes start - [
		// MySQL 에만 존재하는 추가된 데이터 타입	
		
		
		// gssg - 전체적으로 보완_start_20221101
		// gssg - m2m tinytext 딕셔너리 추가
		 else if(_type.equalsIgnoreCase("TINYTEXT")){
			return XLDicInfoCons.TINYTEXT;
		 }
		
		else if(_type.equalsIgnoreCase("MEDIUMTEXT")){
			return XLDicInfoCons.MEDIUMTEXT;
		}
		else if(_type.equalsIgnoreCase("LONGTEXT")){
			return XLDicInfoCons.LONGTEXT;
		}
		else if(_type.equalsIgnoreCase("TINYBLOB")){
			return XLDicInfoCons.TINYBLOB;
		}
		else if(_type.equalsIgnoreCase("MEDIUMBLOB")){
			return XLDicInfoCons.MEDIUMBLOB;
		}
		else if(_type.equalsIgnoreCase("LONGBLOB")){
			return XLDicInfoCons.LONGBLOB;
		}
		else if(_type.equalsIgnoreCase("YEAR")) {
			return XLDicInfoCons.YEAR;
		}
		
		// gssg - 국가정보자원관리원 데이터이관 사업
		// gssg - Altibase to Altibase 지원
		// gssg - Cubrid to Cubrid 지원
		else if(_type.equalsIgnoreCase("VARBIT")) {
			return XLDicInfoCons.VARBIT;
		}
		else if(_type.equalsIgnoreCase("NIBBLE")) {
			return XLDicInfoCons.NIBBLE;
		}
		else if(_type.equalsIgnoreCase("BYTE")) {
			return XLDicInfoCons.BYTE;
		} 
		else if(_type.equalsIgnoreCase("SET")) {
			return XLDicInfoCons.SET;
		} 
		else if(_type.equalsIgnoreCase("MULTISET")) {
			return XLDicInfoCons.MULTISET;
		} 
		else if(_type.equalsIgnoreCase("LIST")) {
			return XLDicInfoCons.LIST;
		} 
		
		// ] - end gssg - for mysql replication - MySQL DataTypes 

		
		//3.2.00-006 2011-07-11 modify add data type - end
		//3.2.00-002 2010-05-19 modify for big type - end
		return XLDicInfoCons.VARCHAR2;
	}
	
}
