package xl.init.main;

public class XLOGCons {
	public static final String STATUS_SUCCESS 	= "S"; // 성공
	public static final String STATUS_FAIL 		= "F"; // 실패
	public static final String STATUS_ABORT 	= "A"; // 중지
	public static final String STATUS_CANCEL	= "C"; // 취소
	public static final String STATUS_WAIT		= "W"; // 대기
	public static final String STATUS_RUNNING	= "R"; // 진행
	
	
	// cksohn - BULK mode oracle sqlldr
	public static final int	   NORMAL_MODE		= 0;
	public static final int	   BULK_MODE		= 1; // sqlldr
	
	// gssg - SK실트론 O2O
	// gssg - linkMode 지원
	public static final int		LINK_MODE		= 2;
	
	

}
