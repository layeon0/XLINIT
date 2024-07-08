package xl.init.info;

import java.util.Vector;

import xl.init.conf.XLConf;

public class XLJobTableInfo {
	
	//src table info
	private String 	sowner = "";
	private String 	stable = "";
	
	
	// tar table info
	private String 	towner = "";
	private String 	ttable = "";
	// gssg - SK��Ʈ�� O2O -- start
	// gssg -  SELECT ���� ����ڰ� ������ �� �״�� ���� ��� ����
	private String		selectScript = "";
	
	// gssg - linkMode ����
	private String		dblinkName = "N";
	// gssg - SK��Ʈ�� O2O -- end
	
	
	// col ���� - �ҽ� ���� colid ������� ����
	private Vector<XLJobColInfo> vtColInfo = new Vector<XLJobColInfo>();
	
	
	// cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ���
	// ���̺��� BigType  �÷� ���� ���� 
	private boolean bigType_yn = false;

	// gssg - SK��Ʈ�� O2O
	// gssg -  SELECT ���� ����ڰ� ������ �� �״�� ���� ��� ����
	// gssg - linkMode ����
	public XLJobTableInfo(String sowner, String stable, String towner, String ttable, String selectScript, String dblinkName) {
		super();
		this.sowner = sowner;
		this.stable = stable;
		this.towner = towner;
		this.ttable = ttable;		
		this.selectScript = selectScript;
		this.dblinkName = dblinkName;
	}

	public String getSowner() {
		return sowner;
	}

	public void setSowner(String sowner) {
		this.sowner = sowner;
	}

	public String getStable() {
		return stable;
	}

	public void setStable(String stable) {
		this.stable = stable;
	}

	public String getTowner() {
		return towner;
	}

	public void setTowner(String towner) {
		this.towner = towner;
	}

	public String getTtable() {
		return ttable;
	}

	public void setTtable(String ttable) {
		this.ttable = ttable;
	}

	// gssg - SK��Ʈ�� O2O -- start
	// gssg -  SELECT ���� ����ڰ� ������ �� �״�� ���� ��� ����
	public String getSelectScript() {
		return selectScript;
	}

	public void setSelectScript(String selectScript) {
		this.selectScript = selectScript;
	}

	// gssg - linkMode ����
	public String getDblinkName() {
		return dblinkName;
	}

	public void setDblinkName(String dblinkName) {
		this.dblinkName = dblinkName;
	}
	// gssg - SK��Ʈ�� O2O -- end

	public Vector<XLJobColInfo> getVtColInfo() {
		return vtColInfo;
	}

	public void setVtColInfo(Vector<XLJobColInfo> vtColInfo) {
		this.vtColInfo = vtColInfo;
	}
		
	
	public void addColInfo(XLJobColInfo _colInfo) {
		this.vtColInfo.add(_colInfo);
	}

	
	// cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ���
	public boolean isBigType_yn() {
		return bigType_yn;
	}

	// cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ���
	public void setBigType_yn(boolean bigType_yn) {
		this.bigType_yn = bigType_yn;
	}


	// cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ���
	// check big type ���� ����
	// gssg - xl t2t ����
//	public void checkBigTypeYN()
	public void checkOraTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				int dataType = this.vtColInfo.get(i).getDataType();
				if ( dataType == XLDicInfoCons.CLOB || 
						dataType == XLDicInfoCons.NCLOB || 
						dataType == XLDicInfoCons.LONG || 
						dataType == XLDicInfoCons.XMLTYPE ||
						dataType == XLDicInfoCons.BLOB || 
						dataType == XLDicInfoCons.LONGRAW || 
						dataType == XLDicInfoCons.BFILE || 
						// raw Ÿ�� �߰�
						dataType == XLDicInfoCons.RAW || 
						// gssg - ��� ����
						// gssg - O2P - 0x00 ������ ó��
						this.vtColInfo.get(i).getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {

					this.bigType_yn = true;
					break;
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// gssg - ���������ڿ������� �������̰� ���
	// gssg - Oracle to Oracle Ÿ���� ó��
	public void checkOraToOraTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				// gssg - �ѱ����� �ҽ� ����Ŭ�� ����
				// gssg - bigType ����ó��
				if ( XLConf.XL_CREATE_FILE_YN ) {
					break;
				}

				
				int dataType = this.vtColInfo.get(i).getDataType();
				if ( dataType == XLDicInfoCons.CLOB || 
						dataType == XLDicInfoCons.NCLOB || 
						dataType == XLDicInfoCons.LONG || 
						dataType == XLDicInfoCons.XMLTYPE ||
						dataType == XLDicInfoCons.BLOB || 
						dataType == XLDicInfoCons.LONGRAW || 
						dataType == XLDicInfoCons.BFILE || 
						dataType == XLDicInfoCons.RAW || 
						dataType == XLDicInfoCons.TIMESTAMP_LTZ ||
						this.vtColInfo.get(i).getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {

					this.bigType_yn = true;
					break;
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// gssg - xl ��ü������ ����
	// gssg - o2o bulk mode raw Ÿ�� ó��
	// gssg - t2o bulk mode raw Ÿ�� ó��
	// gssg - o2m  �ϴٰ� t2t bulk mode ����
	public void checkTiberoTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				int dataType = this.vtColInfo.get(i).getDataType();
				
				// gssg - �ѱ����� �ҽ� ����Ŭ�� ����
				// gssg - bigType ����ó��
				if ( XLConf.XL_CREATE_FILE_YN ) {
					break;
				}
				
				if ( dataType == XLDicInfoCons.CLOB ||
						dataType == XLDicInfoCons.NCLOB ||
						dataType == XLDicInfoCons.LONG ||
						dataType == XLDicInfoCons.BLOB ||
						dataType == XLDicInfoCons.LONGRAW ||
						dataType == XLDicInfoCons.BFILE || 
						dataType == XLDicInfoCons.RAW || 
						dataType == XLDicInfoCons.XMLTYPE || 
						dataType == XLDicInfoCons.INTERVAL_YM || 
						dataType == XLDicInfoCons.INTERVAL_DS || 
						// gssg - �Ｚ���� - 0x00 ������ ó��
						this.vtColInfo.get(i).getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
										
					this.bigType_yn = true;
					break;
			
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		
	}
		
	
	// gssg - o2m  �ϴٰ� p2p bulk mode ����
	public void checkPPASTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				int dataType = this.vtColInfo.get(i).getDataType();
				if ( dataType == XLDicInfoCons.TEXT ||
						dataType == XLDicInfoCons.BYTEA ||
						dataType == XLDicInfoCons.XML ) {
										
					this.bigType_yn = true;
					break;
			
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	// gssg - o2m �ϴٰ� m2m bulk mode ����
	public void checkMySQLTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				int dataType = this.vtColInfo.get(i).getDataType();
				
				if ( dataType == XLDicInfoCons.TEXT ||
						dataType == XLDicInfoCons.MEDIUMTEXT ||
						dataType == XLDicInfoCons.LONGTEXT || 
						dataType == XLDicInfoCons.BLOB || 
						dataType == XLDicInfoCons.MEDIUMBLOB || 
						dataType == XLDicInfoCons.LONGBLOB ) {
										
					this.bigType_yn = true;
					break;
			
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	// gssg - LG���� MS2O
	// gssg - ms2ms normal mode ����
	public void checkMSSQLTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				int dataType = this.vtColInfo.get(i).getDataType();
				if ( dataType == XLDicInfoCons.TEXT ||
						dataType == XLDicInfoCons.NTEXT ||
						dataType == XLDicInfoCons.IMAGE || 
						dataType == XLDicInfoCons.BINARY || 
						dataType == XLDicInfoCons.VARBINARY || 
						dataType == XLDicInfoCons.XML) {
										
					this.bigType_yn = true;
					break;
			
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	
	
}
