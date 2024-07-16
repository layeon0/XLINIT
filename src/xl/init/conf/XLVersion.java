package xl.init.conf;

import xl.init.main.XLOGCons;

public class XLVersion {

/**

		
################################################		
####### 2019_V01 START
################################################		
			
1.0.0.00-001        
        # 2019-05-28
 		[ cksohn - manager 1.0 최초 버전 수정 ]	
 		
 		
################################################		
####### 2019_V02 START
################################################	 		
 		
 		
1.0.0.00-002        
        # 2019-06-13
 		[ cksohn - XL_REGISTER_JOB 기능 추가 ] 	
 		[ cksohn - XL_DELETE_TARGET/XL_TRUNCATE_TARGET 기능 추가 ] 		
 		
 		
1.0.0.00-003 [BULKTEST]  		

[ cksohn - BULK mode oracle sqlldr ]

################################################		
####### 2019_V03_BULK START
################################################	 		

1.0.0.00-004 [BULK]  		
	# 2019-08-16
	#  아래 UI로 부터 request 되는 처리 로직 추가 
	#  단 현재의 base 소스는 BULK 를 테스트했던 소스로 exeMode 	= XLCons.BULK_MODE; 를 default로 셋팅한 모듈로
	#  향후 이 부분을 별도 보완하도록 하고. 
	#  일단은 이 base 소스를 기준으로해서 수정/배포하고,exeMode 	= XLCons.BULK_MODE는 NORMAL_MODE로 하드코딩한상태에서 수정배포한다.
	
	[ cksohn - XL_DELETE_SOURCE/XL_PART_DROP_SOURCE/XL_PART_TRUNC_SOURCE ]

1.0.0.00-005 [BULK]  		
	# 2019-08-19
	# 서버별 동시 최대작업수 체크 오류 
	[ cksohn - 서버별 동시 최대작업수 체크 오류 임시수정 ]

1.0.0.00-006 [BULK]  		
	# 2019-08-23
	[ cksohn - SCHED_ETIME 체크 오류 수정 ]

	
################################################		
####### 2019_V04_BULK START
################################################
1.0.0.00-009 [BULK]  		
	# 2019-09-19
	# LOB Type 지원 (CLOB,BLOB, NCLOB, LONG, LONGRAW, XMLTYPE)
	[ cksohn - xlim LOB 타입 지원 ]


################################################		
####### 2019_V05_BULK START
################################################	
1.0.0.00-010 [BULK]  		
	# 2019-09-24
	# 향후 BULK_MODE는 UI에서 정책별(테이블별로)설정가능하도록 해야 함. 
	# 임시로 XL_CONF 테이블에 설정하도록 함. - 정책별(테이블별)이 아닌 전체로만 설정
	# DBMS 별로 
	[ cksohn - XL_BULK_MODE_YN conf 값 설정 ]
	[ cksohn - XL_BULK_MODE_YN - sqlldr log 파일 지정 및 결과 처리 ]

1.0.0.00-011 [BULK]  		
	# 2019-09-24
    # pipe 사용시, sqlldr가 먼저 수행된후에 Recv 수행되도록 수행순서의 조정이 필요하다
    # sqlldr이 수행되기 전에 Recv가 먼저 끝나버리면, sqlldr은 zombi 프로세스가 된다  
    [ cksohn - XL_BULK_MODE_YN - sqlldr 수행순서 조정 ]


1.0.0.00-012 [BULK]
	# 2019-12-09
	# flashback SCN 지정해서 초기화 가능하도록 
	# XL_INIT_SCN 지정시 기능 동작
	# (예) SELECT ... FROM TBNAME1 AS OF SCN 1234345 WHERE..... 
	# 일단은 이 conf 값은 모든 ILM 대상테이블에 적용되며, 추후 테이블별로 지정할수 있도록 해야 함
	[ cksohn - XL_INIT_SCN ]
	
1.0.0.00-013 [BULK]
	# 2019-12-13	   
	[ cksohn - XL_INIT_SCN - SCN 지정시 테이블 alias e 구문의 Syntax Error 문제 ]   
	
1.0.0.00-014 [BULK]
	# 2020-01-20
	# MGR 라이센스 체크 추가
	[ cksohn - X-LIM checkLicense ]
    
    
################################################		
####### 2020_V01_BULK START
################################################    

1.0.0.00-015 [BULK]
	# 2020-12-16
	# XL_SRC_CHAR_RAWTOHEX_YN=*N|Y
    # XL_SRC_CHAR_ENCODE=MS949 (default) -- XL_ILM_SRC_CHAR_RAWTOHEX_YN=Y 일경우만 의미 있음
    [ cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE ]
    ######################################################
    ---> 이 기능은 추후 BULK MODE 에서 테스트후 release 하도록 함
    ######################################################


################################################		
####### 2019_V06_BULK START
################################################	  

1.0.0.00-012 [BULK]  		
	# 2019-11-04  
	[ cksohn - xl o2p 기능 추가 ]
	
	
################################################		
####### 2021_V01_BULK START
################################################	  

1.0.01-001 [BULK]  		
	# 2021-11-17  
	[ cksohn - xl tibero src 기능 추가 ]	
	
1.0.01-002 [BULK]  		
	# 2021-11-17
	[ cksohn - xl data type 중에 LOB 타입들이 존재하면 BULK_MODE 는 해당 테이블에 대해서 동작하지 않도록 ]  
	
1.0.01-003 [BULK]  		
	# 2021-11-18	
	[ cksohn - xl tibero src 기능 추가 - fetch size 설정 ]
	
1.0.01-004 [BULK]  		
	# 2021-11-18	
	[ cksohn - xl XL_LOB_STREAM_YN=Y|*N ] 
		
1.0.01-005 [BULK]  		
	# 2021-11-19
	[ cksohn - xl oracle bulk mode 데이터 타입별 처리 오류 수정 ]	
	########################################  
	oracle sqlldr bulk mode 에 대한 선능 테스트 필요
	########################################
	
	// CKSOHN DEBUG TEST CODE !!!!!!!!!!!!!
	 
################################################		
####### 2021_V02_BULK START
################################################	 
	
	
1.0.01-006 [BULK]  		
	# 2021-11-20
	[ cksohn - xl bulk mode 성능 개선 - t2o ]
	
1.0.01-007 [BULK]  		
	# 2021-11-26
	# Oracle이 타겟이고 Bulk mode 사용시 데이터 new line 등 처리할 수 잇도록 ctl 파일 및 csv 파일 포맷 수정
	# 신규 conf 값 추가 = XL_BULK_ORACLE_EOL
	[ cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL ]
	

1.0.01-008 [BULK]  		
	# 2021-11-26
	# BULK mode 로 Oracle 반영시 결과는 log 의 메시지로 판단하게 되는데, 
	# XL_BULK_ORACLE_EOL 적용후, 성공시에도 실패로 리포트에 로깅됨. 
	# [ 원인 ]
	# sqlldr 로그 메시지
	# Table XLADMIN.JH_TEST01:
  		2000000 Rows successfully loaded.
  		0 Rows not loaded due to data errors.
  		0 Rows not loaded because all WHEN clauses were failed.
  		1 Row not loaded because all fields were null
    # 여기서 실패된 row는 not loaded 의 건수가 존재하면 실패로 처리하게 되는데,, 
    # XL_BULK_ORACLE_EOL 기능을 적용한후 
    # 1 Row not loaded because all fields were null 는 무조건 발생함. 
    [ cksohn - xl bulk mode for oracle - 결과 처리 오류 수정 ]
	
	
1.0.01-009 [BULK]  		
	# 2021-12-01
	# delete 수행시 NullPointer Exception 발생
	[ cksohn - xl delete 수행시 오류 수정 ]	
	
	
################################################		
####### 2022_V01_BULK START
################################################	

1.0.02-001 [X-LOG MGR]  		
	# 2022-01-12
	# 타겟 Oracle BULK_MODE 수행시 강제종료시 Exception 
	# 특히 sqlldr process가 종료되는지 확인 필요
	[ cksohn - xl BULK_MODE 수행시 강제종료시 Exception 발새 오류 수정 ]
	
	
1.0.02-002 [X-LOG MGR] 		
	# 2022-01-13
	# 타겟 Oracle BULK_MODE 수행시 sqlldr 접속은 SID가 아난 SERVICE NAME 으로 접속하도록 수정
	
	[ cksohn - xl BULK_MODE 수행시 - 타겟 Oracle은 SERVICE NAME 으로 접속 하도록 ]	
	[ cksohn - xl - 수행결과 status log 에 로깅하도록 ]
	

1.0.02-003 [X-LOG MGR] 		
	# 2022-01-13
	# BULK MODE 에서 XL_BULK_ORACLE_EOL 설정값을 '' 로 등록하고 수행할 수 있도록
	# 즉, CHAR/VARCHAR 데이터에 new line 이 없을 경우 위와 같이 사용 가능 
	[ cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL - 미사용 옵션 ]
	
	
################################################		
####### 2022_V02_BULK START
###############################################
	
1.0.02-004 [X-LOG MGR] 		
	# 2022-03-21	
	# XLOG™ 과 같은 special character 문자에 대한 sqlldr (BULK MODE시) 에러 
	# pipe 에 write할때 정확하게 UTF-8 로 write되도록 해 주어야 함 
	[ cksohn - xl bulk mode for oracle - special character loading error ]
	
	
1.0.02-005 [X-LOG MGR] 		
	# 2022-03-29
	# Tibero, Oracle 공백 포함된 컬럼명 문제 
	[ cksohn - xl Tibero, Oracle 공백 포함된 컬럼명 문제  ]	
	
		
################################################		
####### 2022_V03_BULK START
###############################################		
		
1.0.22-001 [X-LOG MGR] 		
	# 2022-04-08
	#  xl gssg 로 최초 이관 ##
	[ gssg - xl m2m 최초 포팅 개발 ]

1.0.22-002 [X-LOG MGR] 		
	# 2022-04-12
	#  xl MySQL, MariaDB Connection 개발 ##
	#  xl MySQL normal mode(Recv, Apply) 개발##
	[ gssg - xl m2m 기능 추가 ]


1.0.22-003 [X-LOG MGR] 		
	# 2022-04-13
	# xl MySQL, MariaDB 완전 분리 ##
	# xl recv type 정리 ## 
	[ gssg - xl m2m 기능 추가  - 0413 ]


1.0.22-004 [X-LOG MGR] 		
	# 2022-04-14
	# xl BIT, DATE type 처리 ##
	[ gssg - xl m2m type 지원 ]


1.0.22-005 [X-LOG MGR] 		
	# 2022-04-15
	# xl m2m UI truncate, delete 기능 추가 ##
	[ gssg - xl m2m UI 지원 ]
	

1.0.22-006 [X-LOG MGR] 		
	# 2022-04-18
	# xl MariaDB 사용을 위해 MySQL 기능 동기화 ##
	[ gssg - xl MariaDB 동기화 ]
	
	
1.0.22-007 [X-LOG MGR] 		
	# 2022-04-19
	# xl m2m bulk mode(Recv, Apply) 개발 ##
	# Loader, ApplyBulk 간 수행 순서 조정 ##
	[ gssg - xl m2m bulk mode 지원 ]

	
1.0.22-008 [X-LOG MGR] 		
	# 2022-04-28 ##
	# xl m2m normal mode blob type 타겟에 정상적으로 반영되도록 어플라이 보완 ##
	# xl m2m bulk mode bit type 지원 ##
	# xl m2m bulk mode "값 정상 복제되도록 replaceCharToCSV_MYSQL 함수 개발 ##
	# xl o2m normal mode blob type 타겟에 대문자로 반영되도록 개발(어플라이에 toUpperCase) ##
	# xl o2m bulk mode lob type 지원되도록 타겟이 mysql이면 bulk mode 동작하도록 개발 ##
	# xl o2m bulk mode "값, (\n, \t 와 같은)특수문자 정상 복제되도록 replaceCharToCSV_OralceToMySQL 함수 개발 ##
	# xl o2m bulk mode 널값 \N 로 지정 ##
	# xl o2m 지원 ##
	# mariadb mysql 동기화 ##
	[ gssg - xl o2m 지원 ]

################################################		
####### 2022_V04_BULK START
###############################################		

1.0.22-001 [X-LOG MGR] 		
	# 2022-05-13
	[ gssg - xl o2m 지원 - 타겟이 마리아일 경우 lob 타입도 bulk mode로 수행하는 기능 원복 - 추후 검증 ]
	[ gssg - xl 카탈로그 mariadb ]
	
	
1.0.22-002 [X-LOG MGR] 		
	# 2022-05-14
	[ gssg - xl 타겟이 MySQL일 경우 bulk mode 컬럼 매핑 지원 ]
	
################################################		
####### 2022_V05_BULK START
###############################################		

1.0.22-001 [X-LOG MGR] 		
	# 2022-05-18
	# xl UI 환경 설정 반영 기능 추가 ##
	# xl mariadb 동기화 ##
	[ gssg - xl UI 기능 추가 및 m2m 보완 ]


1.0.22-009 [X-LOG MGR] 		
	# 2022-05-20
    [ cksohn mysql/maria - java.sql.SQLNonTransientConnectionException: (conn=41) unexpected end of stream, read 0 bytes from 4 이슈 start
    
    
1.0.22-002 [X-LOG MGR] 		
	# 2022-06-10
	# xl recvCnt를 applyCnt로 적용 ##
	# 불필요 기능 주석 처리 ##
	# xl mysql 동기화 ##
	[ gssg - xl m2m bulk mode logging 지원 ]
	

1.0.22-003 [X-LOG MGR] 		
	# 2022-07-14
	# mysql -> ppas 변경 ##
	# ppas copymgr 적용 ##
	# ppas bulk mode 타입 처리 ##
	# ppas apply bulk thread 추가 ##
	# ppas bulk mode logging 지원 ##
	# ppas bulk mode null 처리 ##
	# ppas bulk mode thread 순서 조정 ##
	[ gssg -xl o2p bulk mode 지원 ]
	

1.0.22-004 [X-LOG MGR] 		
	# 2022-08-01
	# 삼성닷컴 xl t2o, o2o 에 대한 LOB 컬럼 이관 이슈 발생 
	LOB 컬럼의 순서가 끝에 있으면 상관 없는데 중간에 있을 때 문제가 됨 
	일단 xl_dicinfo 테이블에 데이터를 수동으로 update 쳐서 문제를 해결 했는데, 
	추후 다시 발생할 문제를 대비하여 엔진 단에서 LOB 컬럼을 뒤로 미루기로 함 ##
	# LOB 컬럼 순서 끝으로 처리 ##
	# o2o LOB 컬럼 순서 끝으로 처리 ##
	# o2p LOB 컬럼 순서 끝으로 처리 ##
	# o2m LOB 컬럼 순서 끝으로 처리 ##
	# t2o LOB 컬럼 순서 끝으로 처리 ##
	# m2m LOB 컬럼 순서 끝으로 처리 ##
	[ gssg - xl lob 타입 보완 ]


1.0.22-005 [X-LOG MGR] 		
	# 2022-08-10
	# o2o bulk mode raw 타입 처리 ##
	# t2o bulk mode raw 타입 처리 ##
	# m2m bulk mode rollback 처리 ##
	# m2m bulk mode thread 순서 조정 ##
	# o2p bulk mode thread 순서 조정 ##
	# m2m bulk mode binary 타입 처리 ##
	[ gssg - xl 전체적으로 보완 ]
	
	
	1.0.22-006 [X-LOG MGR] 		
	# 2022-08-16
	# p2p normal mode 타입 처리 ##
	# p2p bulk mode 타입 처리 ##
	# stopRecvThread() tdbInfo -> sdbInfo ##
	# p2p LOB 컬럼 순서 끝으로 처리 ##
	# p2p bulk mode 스레드 순서 조정 ##
	# p2p 하다가 m2m bulk mode 스레드 순서 조정 ##
	# p2p 하다가 m2m foreign key 무시 설정 추가 ##
	[ gssg - xl p2p 지원 ]


	1.0.22-007 [X-LOG MGR] 		
	# 2022-08-19
	# t2t normal mode 타입 처리 ##
	# t2t bulk mode 지원 ##
	# t2t bulk mode 타입 처리 ##
	# t2t bulk mode 스레드 순서 조정 ##
	# t2t bulk mode lob 타입 지원 ##
	[ gssg - xl t2t 지원 ]
	
	
	1.0.22-008 [X-LOG MGR] 		
	# 2022-09-02
	# t2p bulk mode 타입 처리 ##
	[ gssg - xl t2p 지원 ]
	
	
	1.0.22-009 [X-LOG MGR] 		
	# 2022-09-05
	# o2m bulk mode logging 지원 ##
	# o2m bulk mode 바이너리 타입 보완 ##
	# MariaDB 커넥션 설정 ##
	# o2m 하다가 t2t bulk mode 보완 ##
	# o2m 하다가 p2p bulk mode 보완 ##
	# o2m 하다가 t2p bulk mode 보완 ##
	# o2m 하다가 lob check 보완 ##
	# o2m 하다가 m2m bulk mode 보완 ##
	[ gssg - xl o2m 보완 ]
	
	
	1.0.22-010 [X-LOG MGR] 		
	# 2022-09-19
	# p2t 딕셔너리 정보 수정 ##
	# p2t normal mode 타입 처리 ##
	# p2t bulk mode 타입 처리 ##
	# p2t bulk mode 스레드 순서 조정 ##
	# p2t 하다가 lob check 보완 ##
	# p2t 하다가 o2m time zone 처리 ##
	[ gssg - xl p2t 지원 ]
	
	
	1.0.22-011 [X-LOG MGR] 		
	# 2022-09-29
	# o2p 대소문자 처리 ##
	# o2m 대소문자 처리 ##
	# o2m bulk mode 스레드 순서 조정 ##
	# m2m 대소문자 처리 ##
	# o2p bulk mode 스레드 순서 조정 ##
	[ gssg - xl 전체적으로 보완 ]
	
	
	1.0.22-012 [X-LOG MGR] 		
	# 2022-10-13
	# 정책수동수행 truncate 스레드 순서 조정 ##
	# 정책수동수행 delete 스레드 순서 조정 ##
	# PostgreSQL 커넥터 처리 ##
	# t2p bulk mode 역슬래시 데이터 처리 ##
	[ gssg - xl 전체적으로 보완2 ]


	1.0.22-013 [X-LOG MGR] 		
	# 2022-10-27
	# dicinfo_function 컬럼 추가 ##
	# insert values 부분에 함수 씌우기 ##
	# oracle bulk mode function 지원 ##
	# tibero bulk mode function 지원 ##
	# mysql bulk mode function 지원 ##
	[ gssg - xl function 기능 지원 ]
	

	1.0.22-014 [X-LOG MGR] 		
	# 2022-10-28
	# TIMESTAMP 컬럼 DIC_INFO 처리 ##
	[ gssg - xl PPAS/PostgreSQL 분리 ]
	
	
	1.0.22-015 [X-LOG MGR] 		
	# 2022-11-01
	# m2m bulk mode 역슬래시 데이터 처리 ##
	# m2m tinytext 딕셔너리 추가 ##
	# m2m TIMEZONE 설정 추가 ##
	# potgresql to postgresql date type 처리 ##
	# potgresql to postgresql bulk mode 널값 처리 ##
	# potgresql to postgresql bulk mode 역슬래시 처리 ##
	# potgresql to postgresql time type 처리 ##
	# potgresql to postgresql timestamp_tz type 처리 ##	
	[ gssg - 전체적으로 보완_start_20221101 ]


	1.0.22-016 [X-LOG MGR] 		
	# 2022-11-03
	# UI 동작 처리 ##
	# MSSQL DBMS 추가 ##
	# 바이너리 타입 처리 ##
	# XML 타입 처리 ##
	# IDENTITY 컬럼 처리 ##
	# bulk insert 적용 ##
	# null 처리 ##
	# lob 타입 순서 처리 ##
	# function 지원 ##
	# 정책수동수행 delete 보완 ##
	[ gssg - ms2ms 지원 ]
	
	
	1.0.22-017 [X-LOG MGR] 		
	# 2022-12-01
	# o2t bulk mode 지원 ##
	# interval type 예외처리 ##
	[ gssg - o2t 지원 ]
	
	
	1.0.22-018 [X-LOG MGR] 		
	# 2022-12-08
	# t2p bulk mode 스레드 순서 조정 ##
	# postgreSQL 커넥터 수정 ##
	# t2b normal mode date type 처리 ##
	[ gssg - t2p 보완 ]

###############################################
####### XLManager_2023 START
###############################################		
	
	1.0.23-001 [X-LOG MGR] 		
	# 2022-12-13
	# dicinfo_sec_yn 컬럼 추가 ##
	# XL_TAR_KEYFILE_PATH conf 값 추가 ## 
	# bulk mode thread 순서 조정 ##
	# 타겟 복호화 옵션 추가 ##
	# as of scn 대소문자 구분 처리 ##
	# 현대차 대방동 - addBatch 처리  ##
	# 현대차 대방동 - tbloader 특수문자 처리 ##
	# 현대차 대방동 - tbloader 구문 수정 ##
	# 카카오 - m2m bulk mode 보완 ##
	# 삼성물산 - 0x00 데이터 처리 ##
	# 삼성물산 - 널값 처리 ##
	# 삼성물산 - \N값 처리 ##
		* char 타입의 컬럼에 '\N' 값만 들어 있는 경우 
		* '\N' 문자 자체로 값이 타겟에 이관되어야 하는데, 
		* null 로 들어가는 문제 발생하여 이에 대해 처리함.	
	[ gssg - o2o damo 적용 ]
	
	
	1.0.23-002 [X-LOG MGR] 		
	# 2023-01-25
	# O2P - 0x00 데이터 처리 ##
	# O2P - \N값 처리 ##
	# 우선순위 수정 ##
	# 오라클 Loader - Succes/Fail 판단 수정 ##
		* 아모레퍼시픽에서 스키마구조가 다른 테이블에 
		* 대해 bulk mode 로 이관시 이관에 실패하였는데, 
		* totalCommitCnt : 0, Result - Success 결과가 출력됨.
		* 기존의 Successfully loaded, not loaded 키워드만으로는 
		* 완전한 예외처리가 되지 않아 수정함.
	# P2P - \N값 처리 ##
	# P2P - bulk 스레드 순서 조정 ##
	# P2P - normal NUMERIC 타입 보완 ##
		* 정수/소수 합계 15 자리를 초과하는 숫자에 대해 
		* 16 번째 자리에서 반올림하여 이관되는 문제 발생.
		* ex ) 123.4567890123456 -> 123.456789012346
		* INSERT 구문에 AS NUMERIC 처리함.
	[ gssg - 모듈 보완 ]


	1.0.23-003 [X-LOG MGR]
	# 2023-04-11
	[ gssg - 소스 Function 기능 추가 ]


	1.0.23-004 [X-LOG MGR]
	# 2023-05-02
	* force load 는 이관이 완료된 정책에 대해서 다시 이관을 수행하도록 하는 것을 의미
	# gssg - csv 파일 생성 ##
	# gssg - conf 값 추가 ##
	# gssg - fail 처리 수정 ##
	# gssg - file create 클래스 분리 ##
	# gssg - actian vector csv 처리 ##
	# gssg - thread 순서 조정 ##
	# gssg - recv 건수 기록 ##
	# gssg - loader 수행 안하도록 ##
	# gssg - file 생성이 완료되면 확장자 변경 ##
	[ gssg - csv file create 기능 추가 ]

	
	1.0.23-005 [X-LOG MGR]
	# 2023-05-19
	# gssg - 캐릭터셋 컬럼 데이터 처리 ##
	# gssg - damo api 적용 ##
	# gssg - char, nchar 분리 ##
	[ gssg - damo 캐릭터셋 하드 코딩 보완 ]


	1.0.23-006 [X-LOG MGR]
	# 2023-05-30
	[ gssg - cubrid support ]


	1.0.23-007 [X-LOG MGR]
	# 2023-06-07
	# gssg - MySQL to Tibero 지원 ##
	# gssg - Cubrid to Cubrid 지원 ##
	# gssg - Altibase to Altibase 지원 ##
	# gssg - Altibase to Oracle 지원 ##
	# gssg - 타겟 delete 수행시 싱글 쿼터 에러 보완 ##
	# gssg - Cubrid to Tibero 지원 ##
	# gssg - refactoring ##
	# gssg - PostgreSQL to PostgreSQL 보완 ##
	# gssg - Oracle to Oracle 타임존 처리 ##
	# gssg - Tibero to Tibero 타임존 처리 ##
	# gssg - Tibero to Oracle 타임존 처리 ##
	# gssg - sql loader commit 기능 추가 ##
	# gssg - Altibase5 to Altibase7 지원
	# gssg - Altibase mass lob tmout 보완 ##
	# gssg - tibero tsn 지정 기능 추가 ##
	# gssg - t2p ltz 처리 ##
	# gssg - t2m ltz 처리 ##
	# gssg - t2m bulk mode 지원 ##
	[ gssg - 국가정보자원관리원 데이터이관 사업 ]


	1.0.23-008 [X-LOG MGR]
	# 2023-10-11
	# gssg - smalldatetime 타입 처리 ##
	# gssg - ms2o bulk mode 지원 ##
	# gssg - ms2ms normal mode 지원 ##
	# gssg - o2o bulk tz 보완 ##
	# gssg - o2o bulk ltz 보완 ##
	# gssg - postgresql 커넥션 타임 아웃 보완 ##
	# gssg - 공장코드 값 처리 ##
	# gssg - ora1400 스킵 처리 ##
	# gssg - function 파라미터 여러개 ##
	[ gssg - LG엔솔 MS2O ]


	1.0.24-001 [X-LOG MGR]
	# 2024-02-01
	# gssg -  SELECT 절에 사용자가 정의한 값 그대로 들어가는 기능 지원 #
	# gssg - linkMode 지원 ##
	# gssg - alias 지원 ##
	[ gssg - SK실트론 O2O ]


	1.0.24-002 [X-LOG MGR]
	# 2024-02-13
	# gssg - raw_to_varchar2 기능 지원 #
	[ gssg - 대법원 O2O ]
	
	
	1.0.24-003 [X-LOG MGR]
	# 2024-03-12
	# gssg - oracle fileThread 개발  #
	# gssg - bigType 예외처리 #
	[ gssg - 한국전파 소스 오라클로 변경 ]


	1.0.24-004 [X-LOG MGR]
	# 2024-03-26
	[ gssg - 대법원 rawTovarchar2 수정 ]


	1.0.24-005 [X-LOG MGR]
	# 2024-04-12
	# gssg - 세븐일레븐 O2MS #
	[ gssg - 일본 네트워크 분산 처리 ]
	
	
################################################		
####### 2024_V01_INIT START
###############################################

	1.0.24-001 [X-LOG INIT]
	# 2024-07-01
	# ayzn - XLInit 기능 개발  #
		*기존 IDL XL_JOBQ 테이블 참조 -> CDC 카탈로그 테이블 참조로 변경
		*기존 IDL Polling 방식 -> 일회성 동작으로 변경
		
	# ayzn - XLInit 기능 개발 - conf 수정 : CDC CATALOG 기준으로 변경 #
	# ayzn - XLInit 기능 개발 - conf 수정 : 디버그 모드 true로 변경 #
	# ayzn - XLInit 기능 개발 - conf 수정 : 옵션 처리 추가 #
	# ayzn - XLInit 기능 개발 - conf 수정 : 로그명 포맷 변경 #
	
	# ayzn - XLInit 기능 개발  - InitThread : 받아오는 옵션 값 세팅(정책명, 그룹명, 테이블명) #
	# ayzn - XLInit 기능 개발  - InitThread : 생성자 추가  # 
	# ayzn - XLInit 기능 개발  - InitThread : 기존  polling 방식의 while 주석처리 # 
	# ayzn - XLInit 기능 개발  - InitThread : CNT관련 메모리 주석  # 
	# ayzn - XLInit 기능 개발  - InitThread : IDL의 JOBQ 테이블처리 주석 #  
	# ayzn - XLInit 기능 개발  - InitThread : 1. 소스 DB 정보 세팅 #  
	# ayzn - XLInit 기능 개발  - InitThread : JOBQ -> SOURCE DB 정보 추출  # 
	# ayzn - XLInit 기능 개발  - InitThread :  size < 1 체크 추가 #  
	# ayzn - XLInit 기능 개발  - InitThread : JOBQ -> SOURCE DB 정보 추출 및 HT_JOBQ_DBMS_RCNT_TMP 주석 #  
	# ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는  코드 주석 #  
	# ayzn - XLInit 기능 개발  - InitThread : 함수명 runPol로 변경 및 continue에서 시스템종료로 변경  # 
	# ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는  JOPQ 관련 코드 주석 #  
	# ayzn - XLInit 기능 개발  - InitThread : source db정보로  필요한 정보 세팅 #  
	# ayzn - XLInit 기능 개발  - InitThread : condWhere제외 주석처리 #  
	# ayzn - XLInit 기능 개발  - InitThread : 인자값 수정 #  
	# ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는  cnt메모리 관련 코드 주석 #  
	# ayzn - XLInit 기능 개발  - InitThread : jobseq제외하고 처리 #  
	# ayzn - XLInit 기능 개발  - InitThread : INIT에서 사용하지 않는 코드 주석 #  
	
	# ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobq, cond commit 주석 #
	# ayzn - XLInit 기능 개발  - DB 엔진 수정 : jobseq 제외 #
	# ayzn - XLInit 기능 개발  - DB 엔진 수정 : report, condition, jobq 테이블 관련 처리 주석 #
	
	# ayzn - XLInit 기능 개발  - DBManager : CDC카탈로그 conf 기준으로 변경 #
	# ayzn - XLInit 기능 개발  - DBManager : getDbmsInfo함수 쿼리 CDC 카타로그 테이블 참조로 변경 #
	# ayzn - XLInit 기능 개발  - DBManager : getPolInfo 함수 추가 ( Source, Target(일대다 정책일 시  NR_POL의  DBMS_SEQ 기준) 정보 추출 ) #
	# ayzn - XLInit 기능 개발  - DBManager : getSourceInfo 함수 추가 ( SOURCE 정보 추출 ) #
	# ayzn - XLInit 기능 개발  - DBManager : getJobRunPolInfo 함수  변경 ( CDC카탈로그 참조하여 컬럼정보 추출 ) #
	
	# ayzn - XLInit 기능 개발  - XLJobRunPol : jobseq 주석 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : 옵션 grpcode, tableName 추가 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : source 테이블이름 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : init에서 사용하지않는 file생성기능 주석 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : commit_count, parallel 옵션 처리 추가 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : 수행 대상 정보 세팅 (Source, Target) #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : getJobRunPolInfo 함수처리 시 조건절 추가 및 변경 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : CDC 카탈로그 테이블에 없는 설정값 주석처리 #
	# ayzn - XLInit 기능 개발  - XLJobRunPol : Table 컬럼 정보 생성 시 CDC 카탈로그 테이블 참조로 변경 #
									
	[ ayzn - IDL 코드 기반으로 CDC 테이블  초기화 기능 개발]

**/

	public static String VERSION 	= "1.0.24-001 [X-LOG INIT]";
	public static String BUILD 		= "2024-07-01";
	
}
