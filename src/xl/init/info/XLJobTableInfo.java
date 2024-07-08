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
	// gssg - SK실트론 O2O -- start
	// gssg -  SELECT 절에 사용자가 정의한 값 그대로 들어가는 기능 지원
	private String		selectScript = "";
	
	// gssg - linkMode 지원
	private String		dblinkName = "N";
	// gssg - SK실트론 O2O -- end
	
	
	// col 정보 - 소스 기준 colid 순서대로 관리
	private Vector<XLJobColInfo> vtColInfo = new Vector<XLJobColInfo>();
	
	
	// cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록
	// 테이블의 BigType  컬럼 존재 여부 
	private boolean bigType_yn = false;

	// gssg - SK실트론 O2O
	// gssg -  SELECT 절에 사용자가 정의한 값 그대로 들어가는 기능 지원
	// gssg - linkMode 지원
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

	// gssg - SK실트론 O2O -- start
	// gssg -  SELECT 절에 사용자가 정의한 값 그대로 들어가는 기능 지원
	public String getSelectScript() {
		return selectScript;
	}

	public void setSelectScript(String selectScript) {
		this.selectScript = selectScript;
	}

	// gssg - linkMode 지원
	public String getDblinkName() {
		return dblinkName;
	}

	public void setDblinkName(String dblinkName) {
		this.dblinkName = dblinkName;
	}
	// gssg - SK실트론 O2O -- end

	public Vector<XLJobColInfo> getVtColInfo() {
		return vtColInfo;
	}

	public void setVtColInfo(Vector<XLJobColInfo> vtColInfo) {
		this.vtColInfo = vtColInfo;
	}
		
	
	public void addColInfo(XLJobColInfo _colInfo) {
		this.vtColInfo.add(_colInfo);
	}

	
	// cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록
	public boolean isBigType_yn() {
		return bigType_yn;
	}

	// cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록
	public void setBigType_yn(boolean bigType_yn) {
		this.bigType_yn = bigType_yn;
	}


	// cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록
	// check big type 존재 여부
	// gssg - xl t2t 지원
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
						// raw 타입 추가
						dataType == XLDicInfoCons.RAW || 
						// gssg - 모듈 보완
						// gssg - O2P - 0x00 데이터 처리
						this.vtColInfo.get(i).getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {

					this.bigType_yn = true;
					break;
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// gssg - 국가정보자원관리원 데이터이관 사업
	// gssg - Oracle to Oracle 타임존 처리
	public void checkOraToOraTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				// gssg - 한국전파 소스 오라클로 변경
				// gssg - bigType 예외처리
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
	
	// gssg - xl 전체적으로 보완
	// gssg - o2o bulk mode raw 타입 처리
	// gssg - t2o bulk mode raw 타입 처리
	// gssg - o2m  하다가 t2t bulk mode 보완
	public void checkTiberoTypeYN()
	{
		try {
			
			for ( int i=0; i<this.vtColInfo.size(); i++ ) {
				
				int dataType = this.vtColInfo.get(i).getDataType();
				
				// gssg - 한국전파 소스 오라클로 변경
				// gssg - bigType 예외처리
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
						// gssg - 삼성물산 - 0x00 데이터 처리
						this.vtColInfo.get(i).getFunctionStr().equalsIgnoreCase("XL_REPLACE_NULL") ) {
										
					this.bigType_yn = true;
					break;
			
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		
	}
		
	
	// gssg - o2m  하다가 p2p bulk mode 보완
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

	// gssg - o2m 하다가 m2m bulk mode 보완
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
	
	// gssg - LG엔솔 MS2O
	// gssg - ms2ms normal mode 지원
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
