package xl.init.info;

// gssg - xl m2m type ����

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
		// gssg - xl p2t ����
		// gssg - p2t ��ųʸ� ���� ����
//		else if(_type.equals("TIMESTAMP")||_type.startsWith("TIMESTAMP(")){
		else if(_type.startsWith("TIMESTAMP")){
			//sykim 3.2.01-013 2013-03-07 for add timestamp_ltz
			if(_type.indexOf("LOCAL") > -1){
				return XLDicInfoCons.TIMESTAMP_LTZ;				
			} else if ( _type.indexOf("WITHOUT") > -1) { // cksohn - xl o2p ��� �߰�
				// gssg - xl PPAS/PostgreSQL �и�
				// gssg - TIMESTAMP �÷� DIC_INFO ó��
				return XLDicInfoCons.TIMESTAMP;				
			} else if ( _type.indexOf("ZONE") > -1) { // cksohn - xl o2p ��� �߰�
				// gssg - xl PPAS/PostgreSQL �и�
				// gssg - TIMESTAMP �÷� DIC_INFO ó��
				return XLDicInfoCons.TIMESTAMP_TZ;				
			} else {
				return XLDicInfoCons.TIMESTAMP;
			}		
		}
		else if(_type.equals("NVARCHAR2")){ // cksohn 3.5.00-001 add NVARCHAR2 type
			return XLDicInfoCons.NVARCHAR2;
		}
		else if(_type.equals("NCHAR")){ // cksohn - NCHAR �߰�
			return XLDicInfoCons.NCHAR;
		}
		else if(_type.equals("ROWID")){ // cksohn - ROWID ���� ��� �߰�
			return XLDicInfoCons.ROWID;
		}
		else if(_type.equals("BINARY_FLOAT")){ // cksohn - BINARY_FLOAT/BINARY_DOUBLE ���� ��� �߰�
			return XLDicInfoCons.BINARY_FLOAT;
		}
		else if(_type.equals("BINARY_DOUBLE")){ // cksohn - BINARY_FLOAT/BINARY_DOUBLE ���� ��� �߰�
			return XLDicInfoCons.BINARY_DOUBLE;
		}
		else if(_type.equals("BFILE")){ // cksohn - LOB like type ����
			return XLDicInfoCons.BFILE;
		}
		else if(_type.equals("XMLTYPE")){ // cksohn - LOB like type ����
			return XLDicInfoCons.XMLTYPE;
		}
		// cksohn - INTERVAL type ���� ��� start - [
		else if(_type.startsWith("INTERVAL")){ 
			
			if ( _type.contains("DAY") ){
				return XLDicInfoCons.INTERVAL_DS;
			} else {
				return XLDicInfoCons.INTERVAL_YM;
			}
		}
		// ] - end cksohn - INTERVAL type ���� ���
		
		
		
		// cksohn - for mssql replication - MSSQL DataTypes
		else if(_type.equalsIgnoreCase("BINARY")){ 
			return XLDicInfoCons.BINARY;
		}
		else if(_type.equalsIgnoreCase("BIT")){ 
			return XLDicInfoCons.BIT;
		}
		else if(_type.equalsIgnoreCase("DATETIME")){
			// gssg - LG���� MS2O
			// gssg - ms2o bulk mode ����
//			return XLDicInfoCons.DATETIME2;
			return XLDicInfoCons.DATETIME;
		}
		else if(_type.equalsIgnoreCase("DATETIME2")){ 
			return XLDicInfoCons.DATETIME2;
		}
		// else if(_type.equalsIgnoreCase("DATETIMEOFFSEET")){ 
		else if(_type.equalsIgnoreCase("DATETIMEOFFSET")){ // cksohn - DATETIMEOFFSET, TIME ���� ����
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
		
		
		else if(_type.toUpperCase().startsWith("TIME(") || _type.equalsIgnoreCase("TIME")){ // time(7) // cksohn - DATETIMEOFFSET, TIME ���� ����
			return XLDicInfoCons.TIME;
		}

		// gssg - ��ü������ ����_start_20221101
		// gssg - potgresql to postgresql time type ó��
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
		// gssg - xl p2p ����
		// gssg - p2p normal mode Ÿ�� ó��
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

		// gssg - xl m2m type ����
		// gssg - for mysql replication - MySQL DataTypes start - [
		// MySQL ���� �����ϴ� �߰��� ������ Ÿ��	
		
		
		// gssg - ��ü������ ����_start_20221101
		// gssg - m2m tinytext ��ųʸ� �߰�
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
		
		// gssg - ���������ڿ������� �������̰� ���
		// gssg - Altibase to Altibase ����
		// gssg - Cubrid to Cubrid ����
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
