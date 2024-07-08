package xl.init.info;

import xl.init.dbmgr.XLDBDataTypes;


public class XLJobColInfo {
	
	private String colName = ""; // 소스 컬럼명
	private String colName_map = ""; // 타겟 컬럼명
	private int dataType = XLDBDataTypes.VARCHAR2;
	private int colId = 1; // 소스 테이블 기준 colid
	private String logmnrYN = "N";
	private String functionStr = "";
	// gssg - 소스 Function 기능 추가
	private String functionStrSrc = "";
	private String secYN = "N";
	private String secMapYN = "N";
	
	// gssg - xl function 기능 지원
	// gssg - dicinfo_function 컬럼 추가
	// gssg - o2o damo 적용
	// gssg - dicinfo_sec_yn 컬럼 추가
	// gssg - 소스 Function 기능 추가
	public XLJobColInfo(String _colName, String _colName_map, int _dataType, int _colId, String _logmnrYN, String _functionStr, String _functionStrSrc, String _secYN, String _secMapYN) {
		this.colName = _colName;
		this.colName_map = _colName_map;
		this.dataType = _dataType;
		this.colId = _colId;
		this.logmnrYN = _logmnrYN;
		this.functionStr = _functionStr;
		this.functionStrSrc = _functionStrSrc;
		this.secYN = _secYN;
		this.secMapYN = _secMapYN;
	}

	public String getColName() {
		return colName;
	}

	public void setColName(String colName) {
		this.colName = colName;
	}

	public String getColName_map() {
		return colName_map;
	}

	public void setColName_map(String colName_map) {
		this.colName_map = colName_map;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public int getColId() {
		return colId;
	}

	public void setColId(int colId) {
		this.colId = colId;
	}

	public String getLogmnrYN() {
		return logmnrYN;
	}

	public void setLogmnrYN(String logmnrYN) {
		this.logmnrYN = logmnrYN;
	}

	// gssg - xl function 기능 지원
	// gssg - dicinfo_function 컬럼 추가
	public String getFunctionStr() {
		return functionStr;
	}
	public void setFunctionStr(String functionStr) {
		this.functionStr = functionStr;
	}
	
	// gssg - 소스 Function 기능 추가
	public String getFunctionStrSrc() {
		return functionStrSrc;
	}

	public void setFunctionStrSrc(String functionStrSrc) {
		this.functionStrSrc = functionStrSrc;
	}

	// gssg - o2o damo 적용
	// gssg - dicinfo_sec_yn 컬럼 추가
	public String getSecYN() {
		return secYN;
	}

	public void setSecYN(String secYN) {
		this.secYN = secYN;
	}

	public String getSecMapYN() {
		return secMapYN;
	}

	public void setSecMapYN(String secMapYN) {
		this.secMapYN = secMapYN;
	}
	
	
}
