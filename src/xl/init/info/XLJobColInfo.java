package xl.init.info;

import xl.init.dbmgr.XLDBDataTypes;


public class XLJobColInfo {
	
	private String colName = ""; // �ҽ� �÷���
	private String colName_map = ""; // Ÿ�� �÷���
	private int dataType = XLDBDataTypes.VARCHAR2;
	private int colId = 1; // �ҽ� ���̺� ���� colid
	private String logmnrYN = "N";
	private String functionStr = "";
	// gssg - �ҽ� Function ��� �߰�
	private String functionStrSrc = "";
	private String secYN = "N";
	private String secMapYN = "N";
	
	// gssg - xl function ��� ����
	// gssg - dicinfo_function �÷� �߰�
	// gssg - o2o damo ����
	// gssg - dicinfo_sec_yn �÷� �߰�
	// gssg - �ҽ� Function ��� �߰�
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

	// gssg - xl function ��� ����
	// gssg - dicinfo_function �÷� �߰�
	public String getFunctionStr() {
		return functionStr;
	}
	public void setFunctionStr(String functionStr) {
		this.functionStr = functionStr;
	}
	
	// gssg - �ҽ� Function ��� �߰�
	public String getFunctionStrSrc() {
		return functionStrSrc;
	}

	public void setFunctionStrSrc(String functionStrSrc) {
		this.functionStrSrc = functionStrSrc;
	}

	// gssg - o2o damo ����
	// gssg - dicinfo_sec_yn �÷� �߰�
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
