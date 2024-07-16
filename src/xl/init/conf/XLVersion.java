package xl.init.conf;

import xl.init.main.XLOGCons;

public class XLVersion {

/**

		
################################################		
####### 2019_V01 START
################################################		
			
1.0.0.00-001        
        # 2019-05-28
 		[ cksohn - manager 1.0 ���� ���� ���� ]	
 		
 		
################################################		
####### 2019_V02 START
################################################	 		
 		
 		
1.0.0.00-002        
        # 2019-06-13
 		[ cksohn - XL_REGISTER_JOB ��� �߰� ] 	
 		[ cksohn - XL_DELETE_TARGET/XL_TRUNCATE_TARGET ��� �߰� ] 		
 		
 		
1.0.0.00-003 [BULKTEST]  		

[ cksohn - BULK mode oracle sqlldr ]

################################################		
####### 2019_V03_BULK START
################################################	 		

1.0.0.00-004 [BULK]  		
	# 2019-08-16
	#  �Ʒ� UI�� ���� request �Ǵ� ó�� ���� �߰� 
	#  �� ������ base �ҽ��� BULK �� �׽�Ʈ�ߴ� �ҽ��� exeMode 	= XLCons.BULK_MODE; �� default�� ������ ����
	#  ���� �� �κ��� ���� �����ϵ��� �ϰ�. 
	#  �ϴ��� �� base �ҽ��� ���������ؼ� ����/�����ϰ�,exeMode 	= XLCons.BULK_MODE�� NORMAL_MODE�� �ϵ��ڵ��ѻ��¿��� ���������Ѵ�.
	
	[ cksohn - XL_DELETE_SOURCE/XL_PART_DROP_SOURCE/XL_PART_TRUNC_SOURCE ]

1.0.0.00-005 [BULK]  		
	# 2019-08-19
	# ������ ���� �ִ��۾��� üũ ���� 
	[ cksohn - ������ ���� �ִ��۾��� üũ ���� �ӽü��� ]

1.0.0.00-006 [BULK]  		
	# 2019-08-23
	[ cksohn - SCHED_ETIME üũ ���� ���� ]

	
################################################		
####### 2019_V04_BULK START
################################################
1.0.0.00-009 [BULK]  		
	# 2019-09-19
	# LOB Type ���� (CLOB,BLOB, NCLOB, LONG, LONGRAW, XMLTYPE)
	[ cksohn - xlim LOB Ÿ�� ���� ]


################################################		
####### 2019_V05_BULK START
################################################	
1.0.0.00-010 [BULK]  		
	# 2019-09-24
	# ���� BULK_MODE�� UI���� ��å��(���̺���)���������ϵ��� �ؾ� ��. 
	# �ӽ÷� XL_CONF ���̺� �����ϵ��� ��. - ��å��(���̺�)�� �ƴ� ��ü�θ� ����
	# DBMS ���� 
	[ cksohn - XL_BULK_MODE_YN conf �� ���� ]
	[ cksohn - XL_BULK_MODE_YN - sqlldr log ���� ���� �� ��� ó�� ]

1.0.0.00-011 [BULK]  		
	# 2019-09-24
    # pipe ����, sqlldr�� ���� ������Ŀ� Recv ����ǵ��� ��������� ������ �ʿ��ϴ�
    # sqlldr�� ����Ǳ� ���� Recv�� ���� ����������, sqlldr�� zombi ���μ����� �ȴ�  
    [ cksohn - XL_BULK_MODE_YN - sqlldr ������� ���� ]


1.0.0.00-012 [BULK]
	# 2019-12-09
	# flashback SCN �����ؼ� �ʱ�ȭ �����ϵ��� 
	# XL_INIT_SCN ������ ��� ����
	# (��) SELECT ... FROM TBNAME1 AS OF SCN 1234345 WHERE..... 
	# �ϴ��� �� conf ���� ��� ILM ������̺� ����Ǹ�, ���� ���̺��� �����Ҽ� �ֵ��� �ؾ� ��
	[ cksohn - XL_INIT_SCN ]
	
1.0.0.00-013 [BULK]
	# 2019-12-13	   
	[ cksohn - XL_INIT_SCN - SCN ������ ���̺� alias e ������ Syntax Error ���� ]   
	
1.0.0.00-014 [BULK]
	# 2020-01-20
	# MGR ���̼��� üũ �߰�
	[ cksohn - X-LIM checkLicense ]
    
    
################################################		
####### 2020_V01_BULK START
################################################    

1.0.0.00-015 [BULK]
	# 2020-12-16
	# XL_SRC_CHAR_RAWTOHEX_YN=*N|Y
    # XL_SRC_CHAR_ENCODE=MS949 (default) -- XL_ILM_SRC_CHAR_RAWTOHEX_YN=Y �ϰ�츸 �ǹ� ����
    [ cksohn - XL_SRC_CHAR_RAWTOHEX_YN / XL_SRC_CHAR_ENCODE ]
    ######################################################
    ---> �� ����� ���� BULK MODE ���� �׽�Ʈ�� release �ϵ��� ��
    ######################################################


################################################		
####### 2019_V06_BULK START
################################################	  

1.0.0.00-012 [BULK]  		
	# 2019-11-04  
	[ cksohn - xl o2p ��� �߰� ]
	
	
################################################		
####### 2021_V01_BULK START
################################################	  

1.0.01-001 [BULK]  		
	# 2021-11-17  
	[ cksohn - xl tibero src ��� �߰� ]	
	
1.0.01-002 [BULK]  		
	# 2021-11-17
	[ cksohn - xl data type �߿� LOB Ÿ�Ե��� �����ϸ� BULK_MODE �� �ش� ���̺� ���ؼ� �������� �ʵ��� ]  
	
1.0.01-003 [BULK]  		
	# 2021-11-18	
	[ cksohn - xl tibero src ��� �߰� - fetch size ���� ]
	
1.0.01-004 [BULK]  		
	# 2021-11-18	
	[ cksohn - xl XL_LOB_STREAM_YN=Y|*N ] 
		
1.0.01-005 [BULK]  		
	# 2021-11-19
	[ cksohn - xl oracle bulk mode ������ Ÿ�Ժ� ó�� ���� ���� ]	
	########################################  
	oracle sqlldr bulk mode �� ���� ���� �׽�Ʈ �ʿ�
	########################################
	
	// CKSOHN DEBUG TEST CODE !!!!!!!!!!!!!
	 
################################################		
####### 2021_V02_BULK START
################################################	 
	
	
1.0.01-006 [BULK]  		
	# 2021-11-20
	[ cksohn - xl bulk mode ���� ���� - t2o ]
	
1.0.01-007 [BULK]  		
	# 2021-11-26
	# Oracle�� Ÿ���̰� Bulk mode ���� ������ new line �� ó���� �� �յ��� ctl ���� �� csv ���� ���� ����
	# �ű� conf �� �߰� = XL_BULK_ORACLE_EOL
	[ cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL ]
	

1.0.01-008 [BULK]  		
	# 2021-11-26
	# BULK mode �� Oracle �ݿ��� ����� log �� �޽����� �Ǵ��ϰ� �Ǵµ�, 
	# XL_BULK_ORACLE_EOL ������, �����ÿ��� ���з� ����Ʈ�� �α��. 
	# [ ���� ]
	# sqlldr �α� �޽���
	# Table XLADMIN.JH_TEST01:
  		2000000 Rows successfully loaded.
  		0 Rows not loaded due to data errors.
  		0 Rows not loaded because all WHEN clauses were failed.
  		1 Row not loaded because all fields were null
    # ���⼭ ���е� row�� not loaded �� �Ǽ��� �����ϸ� ���з� ó���ϰ� �Ǵµ�,, 
    # XL_BULK_ORACLE_EOL ����� �������� 
    # 1 Row not loaded because all fields were null �� ������ �߻���. 
    [ cksohn - xl bulk mode for oracle - ��� ó�� ���� ���� ]
	
	
1.0.01-009 [BULK]  		
	# 2021-12-01
	# delete ����� NullPointer Exception �߻�
	[ cksohn - xl delete ����� ���� ���� ]	
	
	
################################################		
####### 2022_V01_BULK START
################################################	

1.0.02-001 [X-LOG MGR]  		
	# 2022-01-12
	# Ÿ�� Oracle BULK_MODE ����� ��������� Exception 
	# Ư�� sqlldr process�� ����Ǵ��� Ȯ�� �ʿ�
	[ cksohn - xl BULK_MODE ����� ��������� Exception �߻� ���� ���� ]
	
	
1.0.02-002 [X-LOG MGR] 		
	# 2022-01-13
	# Ÿ�� Oracle BULK_MODE ����� sqlldr ������ SID�� �Ƴ� SERVICE NAME ���� �����ϵ��� ����
	
	[ cksohn - xl BULK_MODE ����� - Ÿ�� Oracle�� SERVICE NAME ���� ���� �ϵ��� ]	
	[ cksohn - xl - ������ status log �� �α��ϵ��� ]
	

1.0.02-003 [X-LOG MGR] 		
	# 2022-01-13
	# BULK MODE ���� XL_BULK_ORACLE_EOL �������� '' �� ����ϰ� ������ �� �ֵ���
	# ��, CHAR/VARCHAR �����Ϳ� new line �� ���� ��� ���� ���� ��� ���� 
	[ cksohn - xl bulk mode for oracle - XL_BULK_ORACLE_EOL - �̻�� �ɼ� ]
	
	
################################################		
####### 2022_V02_BULK START
###############################################
	
1.0.02-004 [X-LOG MGR] 		
	# 2022-03-21	
	# XLOG�� �� ���� special character ���ڿ� ���� sqlldr (BULK MODE��) ���� 
	# pipe �� write�Ҷ� ��Ȯ�ϰ� UTF-8 �� write�ǵ��� �� �־�� �� 
	[ cksohn - xl bulk mode for oracle - special character loading error ]
	
	
1.0.02-005 [X-LOG MGR] 		
	# 2022-03-29
	# Tibero, Oracle ���� ���Ե� �÷��� ���� 
	[ cksohn - xl Tibero, Oracle ���� ���Ե� �÷��� ����  ]	
	
		
################################################		
####### 2022_V03_BULK START
###############################################		
		
1.0.22-001 [X-LOG MGR] 		
	# 2022-04-08
	#  xl gssg �� ���� �̰� ##
	[ gssg - xl m2m ���� ���� ���� ]

1.0.22-002 [X-LOG MGR] 		
	# 2022-04-12
	#  xl MySQL, MariaDB Connection ���� ##
	#  xl MySQL normal mode(Recv, Apply) ����##
	[ gssg - xl m2m ��� �߰� ]


1.0.22-003 [X-LOG MGR] 		
	# 2022-04-13
	# xl MySQL, MariaDB ���� �и� ##
	# xl recv type ���� ## 
	[ gssg - xl m2m ��� �߰�  - 0413 ]


1.0.22-004 [X-LOG MGR] 		
	# 2022-04-14
	# xl BIT, DATE type ó�� ##
	[ gssg - xl m2m type ���� ]


1.0.22-005 [X-LOG MGR] 		
	# 2022-04-15
	# xl m2m UI truncate, delete ��� �߰� ##
	[ gssg - xl m2m UI ���� ]
	

1.0.22-006 [X-LOG MGR] 		
	# 2022-04-18
	# xl MariaDB ����� ���� MySQL ��� ����ȭ ##
	[ gssg - xl MariaDB ����ȭ ]
	
	
1.0.22-007 [X-LOG MGR] 		
	# 2022-04-19
	# xl m2m bulk mode(Recv, Apply) ���� ##
	# Loader, ApplyBulk �� ���� ���� ���� ##
	[ gssg - xl m2m bulk mode ���� ]

	
1.0.22-008 [X-LOG MGR] 		
	# 2022-04-28 ##
	# xl m2m normal mode blob type Ÿ�ٿ� ���������� �ݿ��ǵ��� ���ö��� ���� ##
	# xl m2m bulk mode bit type ���� ##
	# xl m2m bulk mode "�� ���� �����ǵ��� replaceCharToCSV_MYSQL �Լ� ���� ##
	# xl o2m normal mode blob type Ÿ�ٿ� �빮�ڷ� �ݿ��ǵ��� ����(���ö��̿� toUpperCase) ##
	# xl o2m bulk mode lob type �����ǵ��� Ÿ���� mysql�̸� bulk mode �����ϵ��� ���� ##
	# xl o2m bulk mode "��, (\n, \t �� ����)Ư������ ���� �����ǵ��� replaceCharToCSV_OralceToMySQL �Լ� ���� ##
	# xl o2m bulk mode �ΰ� \N �� ���� ##
	# xl o2m ���� ##
	# mariadb mysql ����ȭ ##
	[ gssg - xl o2m ���� ]

################################################		
####### 2022_V04_BULK START
###############################################		

1.0.22-001 [X-LOG MGR] 		
	# 2022-05-13
	[ gssg - xl o2m ���� - Ÿ���� �������� ��� lob Ÿ�Ե� bulk mode�� �����ϴ� ��� ���� - ���� ���� ]
	[ gssg - xl īŻ�α� mariadb ]
	
	
1.0.22-002 [X-LOG MGR] 		
	# 2022-05-14
	[ gssg - xl Ÿ���� MySQL�� ��� bulk mode �÷� ���� ���� ]
	
################################################		
####### 2022_V05_BULK START
###############################################		

1.0.22-001 [X-LOG MGR] 		
	# 2022-05-18
	# xl UI ȯ�� ���� �ݿ� ��� �߰� ##
	# xl mariadb ����ȭ ##
	[ gssg - xl UI ��� �߰� �� m2m ���� ]


1.0.22-009 [X-LOG MGR] 		
	# 2022-05-20
    [ cksohn mysql/maria - java.sql.SQLNonTransientConnectionException: (conn=41) unexpected end of stream, read 0 bytes from 4 �̽� start
    
    
1.0.22-002 [X-LOG MGR] 		
	# 2022-06-10
	# xl recvCnt�� applyCnt�� ���� ##
	# ���ʿ� ��� �ּ� ó�� ##
	# xl mysql ����ȭ ##
	[ gssg - xl m2m bulk mode logging ���� ]
	

1.0.22-003 [X-LOG MGR] 		
	# 2022-07-14
	# mysql -> ppas ���� ##
	# ppas copymgr ���� ##
	# ppas bulk mode Ÿ�� ó�� ##
	# ppas apply bulk thread �߰� ##
	# ppas bulk mode logging ���� ##
	# ppas bulk mode null ó�� ##
	# ppas bulk mode thread ���� ���� ##
	[ gssg -xl o2p bulk mode ���� ]
	

1.0.22-004 [X-LOG MGR] 		
	# 2022-08-01
	# �Ｚ���� xl t2o, o2o �� ���� LOB �÷� �̰� �̽� �߻� 
	LOB �÷��� ������ ���� ������ ��� ���µ� �߰��� ���� �� ������ �� 
	�ϴ� xl_dicinfo ���̺� �����͸� �������� update �ļ� ������ �ذ� �ߴµ�, 
	���� �ٽ� �߻��� ������ ����Ͽ� ���� �ܿ��� LOB �÷��� �ڷ� �̷��� �� ##
	# LOB �÷� ���� ������ ó�� ##
	# o2o LOB �÷� ���� ������ ó�� ##
	# o2p LOB �÷� ���� ������ ó�� ##
	# o2m LOB �÷� ���� ������ ó�� ##
	# t2o LOB �÷� ���� ������ ó�� ##
	# m2m LOB �÷� ���� ������ ó�� ##
	[ gssg - xl lob Ÿ�� ���� ]


1.0.22-005 [X-LOG MGR] 		
	# 2022-08-10
	# o2o bulk mode raw Ÿ�� ó�� ##
	# t2o bulk mode raw Ÿ�� ó�� ##
	# m2m bulk mode rollback ó�� ##
	# m2m bulk mode thread ���� ���� ##
	# o2p bulk mode thread ���� ���� ##
	# m2m bulk mode binary Ÿ�� ó�� ##
	[ gssg - xl ��ü������ ���� ]
	
	
	1.0.22-006 [X-LOG MGR] 		
	# 2022-08-16
	# p2p normal mode Ÿ�� ó�� ##
	# p2p bulk mode Ÿ�� ó�� ##
	# stopRecvThread() tdbInfo -> sdbInfo ##
	# p2p LOB �÷� ���� ������ ó�� ##
	# p2p bulk mode ������ ���� ���� ##
	# p2p �ϴٰ� m2m bulk mode ������ ���� ���� ##
	# p2p �ϴٰ� m2m foreign key ���� ���� �߰� ##
	[ gssg - xl p2p ���� ]


	1.0.22-007 [X-LOG MGR] 		
	# 2022-08-19
	# t2t normal mode Ÿ�� ó�� ##
	# t2t bulk mode ���� ##
	# t2t bulk mode Ÿ�� ó�� ##
	# t2t bulk mode ������ ���� ���� ##
	# t2t bulk mode lob Ÿ�� ���� ##
	[ gssg - xl t2t ���� ]
	
	
	1.0.22-008 [X-LOG MGR] 		
	# 2022-09-02
	# t2p bulk mode Ÿ�� ó�� ##
	[ gssg - xl t2p ���� ]
	
	
	1.0.22-009 [X-LOG MGR] 		
	# 2022-09-05
	# o2m bulk mode logging ���� ##
	# o2m bulk mode ���̳ʸ� Ÿ�� ���� ##
	# MariaDB Ŀ�ؼ� ���� ##
	# o2m �ϴٰ� t2t bulk mode ���� ##
	# o2m �ϴٰ� p2p bulk mode ���� ##
	# o2m �ϴٰ� t2p bulk mode ���� ##
	# o2m �ϴٰ� lob check ���� ##
	# o2m �ϴٰ� m2m bulk mode ���� ##
	[ gssg - xl o2m ���� ]
	
	
	1.0.22-010 [X-LOG MGR] 		
	# 2022-09-19
	# p2t ��ųʸ� ���� ���� ##
	# p2t normal mode Ÿ�� ó�� ##
	# p2t bulk mode Ÿ�� ó�� ##
	# p2t bulk mode ������ ���� ���� ##
	# p2t �ϴٰ� lob check ���� ##
	# p2t �ϴٰ� o2m time zone ó�� ##
	[ gssg - xl p2t ���� ]
	
	
	1.0.22-011 [X-LOG MGR] 		
	# 2022-09-29
	# o2p ��ҹ��� ó�� ##
	# o2m ��ҹ��� ó�� ##
	# o2m bulk mode ������ ���� ���� ##
	# m2m ��ҹ��� ó�� ##
	# o2p bulk mode ������ ���� ���� ##
	[ gssg - xl ��ü������ ���� ]
	
	
	1.0.22-012 [X-LOG MGR] 		
	# 2022-10-13
	# ��å�������� truncate ������ ���� ���� ##
	# ��å�������� delete ������ ���� ���� ##
	# PostgreSQL Ŀ���� ó�� ##
	# t2p bulk mode �������� ������ ó�� ##
	[ gssg - xl ��ü������ ����2 ]


	1.0.22-013 [X-LOG MGR] 		
	# 2022-10-27
	# dicinfo_function �÷� �߰� ##
	# insert values �κп� �Լ� ����� ##
	# oracle bulk mode function ���� ##
	# tibero bulk mode function ���� ##
	# mysql bulk mode function ���� ##
	[ gssg - xl function ��� ���� ]
	

	1.0.22-014 [X-LOG MGR] 		
	# 2022-10-28
	# TIMESTAMP �÷� DIC_INFO ó�� ##
	[ gssg - xl PPAS/PostgreSQL �и� ]
	
	
	1.0.22-015 [X-LOG MGR] 		
	# 2022-11-01
	# m2m bulk mode �������� ������ ó�� ##
	# m2m tinytext ��ųʸ� �߰� ##
	# m2m TIMEZONE ���� �߰� ##
	# potgresql to postgresql date type ó�� ##
	# potgresql to postgresql bulk mode �ΰ� ó�� ##
	# potgresql to postgresql bulk mode �������� ó�� ##
	# potgresql to postgresql time type ó�� ##
	# potgresql to postgresql timestamp_tz type ó�� ##	
	[ gssg - ��ü������ ����_start_20221101 ]


	1.0.22-016 [X-LOG MGR] 		
	# 2022-11-03
	# UI ���� ó�� ##
	# MSSQL DBMS �߰� ##
	# ���̳ʸ� Ÿ�� ó�� ##
	# XML Ÿ�� ó�� ##
	# IDENTITY �÷� ó�� ##
	# bulk insert ���� ##
	# null ó�� ##
	# lob Ÿ�� ���� ó�� ##
	# function ���� ##
	# ��å�������� delete ���� ##
	[ gssg - ms2ms ���� ]
	
	
	1.0.22-017 [X-LOG MGR] 		
	# 2022-12-01
	# o2t bulk mode ���� ##
	# interval type ����ó�� ##
	[ gssg - o2t ���� ]
	
	
	1.0.22-018 [X-LOG MGR] 		
	# 2022-12-08
	# t2p bulk mode ������ ���� ���� ##
	# postgreSQL Ŀ���� ���� ##
	# t2b normal mode date type ó�� ##
	[ gssg - t2p ���� ]

###############################################
####### XLManager_2023 START
###############################################		
	
	1.0.23-001 [X-LOG MGR] 		
	# 2022-12-13
	# dicinfo_sec_yn �÷� �߰� ##
	# XL_TAR_KEYFILE_PATH conf �� �߰� ## 
	# bulk mode thread ���� ���� ##
	# Ÿ�� ��ȣȭ �ɼ� �߰� ##
	# as of scn ��ҹ��� ���� ó�� ##
	# ������ ��浿 - addBatch ó��  ##
	# ������ ��浿 - tbloader Ư������ ó�� ##
	# ������ ��浿 - tbloader ���� ���� ##
	# īī�� - m2m bulk mode ���� ##
	# �Ｚ���� - 0x00 ������ ó�� ##
	# �Ｚ���� - �ΰ� ó�� ##
	# �Ｚ���� - \N�� ó�� ##
		* char Ÿ���� �÷��� '\N' ���� ��� �ִ� ��� 
		* '\N' ���� ��ü�� ���� Ÿ�ٿ� �̰��Ǿ�� �ϴµ�, 
		* null �� ���� ���� �߻��Ͽ� �̿� ���� ó����.	
	[ gssg - o2o damo ���� ]
	
	
	1.0.23-002 [X-LOG MGR] 		
	# 2023-01-25
	# O2P - 0x00 ������ ó�� ##
	# O2P - \N�� ó�� ##
	# �켱���� ���� ##
	# ����Ŭ Loader - Succes/Fail �Ǵ� ���� ##
		* �Ƹ��۽��ȿ��� ��Ű�������� �ٸ� ���̺� 
		* ���� bulk mode �� �̰��� �̰��� �����Ͽ��µ�, 
		* totalCommitCnt : 0, Result - Success ����� ��µ�.
		* ������ Successfully loaded, not loaded Ű���常���δ� 
		* ������ ����ó���� ���� �ʾ� ������.
	# P2P - \N�� ó�� ##
	# P2P - bulk ������ ���� ���� ##
	# P2P - normal NUMERIC Ÿ�� ���� ##
		* ����/�Ҽ� �հ� 15 �ڸ��� �ʰ��ϴ� ���ڿ� ���� 
		* 16 ��° �ڸ����� �ݿø��Ͽ� �̰��Ǵ� ���� �߻�.
		* ex ) 123.4567890123456 -> 123.456789012346
		* INSERT ������ AS NUMERIC ó����.
	[ gssg - ��� ���� ]


	1.0.23-003 [X-LOG MGR]
	# 2023-04-11
	[ gssg - �ҽ� Function ��� �߰� ]


	1.0.23-004 [X-LOG MGR]
	# 2023-05-02
	* force load �� �̰��� �Ϸ�� ��å�� ���ؼ� �ٽ� �̰��� �����ϵ��� �ϴ� ���� �ǹ�
	# gssg - csv ���� ���� ##
	# gssg - conf �� �߰� ##
	# gssg - fail ó�� ���� ##
	# gssg - file create Ŭ���� �и� ##
	# gssg - actian vector csv ó�� ##
	# gssg - thread ���� ���� ##
	# gssg - recv �Ǽ� ��� ##
	# gssg - loader ���� ���ϵ��� ##
	# gssg - file ������ �Ϸ�Ǹ� Ȯ���� ���� ##
	[ gssg - csv file create ��� �߰� ]

	
	1.0.23-005 [X-LOG MGR]
	# 2023-05-19
	# gssg - ĳ���ͼ� �÷� ������ ó�� ##
	# gssg - damo api ���� ##
	# gssg - char, nchar �и� ##
	[ gssg - damo ĳ���ͼ� �ϵ� �ڵ� ���� ]


	1.0.23-006 [X-LOG MGR]
	# 2023-05-30
	[ gssg - cubrid support ]


	1.0.23-007 [X-LOG MGR]
	# 2023-06-07
	# gssg - MySQL to Tibero ���� ##
	# gssg - Cubrid to Cubrid ���� ##
	# gssg - Altibase to Altibase ���� ##
	# gssg - Altibase to Oracle ���� ##
	# gssg - Ÿ�� delete ����� �̱� ���� ���� ���� ##
	# gssg - Cubrid to Tibero ���� ##
	# gssg - refactoring ##
	# gssg - PostgreSQL to PostgreSQL ���� ##
	# gssg - Oracle to Oracle Ÿ���� ó�� ##
	# gssg - Tibero to Tibero Ÿ���� ó�� ##
	# gssg - Tibero to Oracle Ÿ���� ó�� ##
	# gssg - sql loader commit ��� �߰� ##
	# gssg - Altibase5 to Altibase7 ����
	# gssg - Altibase mass lob tmout ���� ##
	# gssg - tibero tsn ���� ��� �߰� ##
	# gssg - t2p ltz ó�� ##
	# gssg - t2m ltz ó�� ##
	# gssg - t2m bulk mode ���� ##
	[ gssg - ���������ڿ������� �������̰� ��� ]


	1.0.23-008 [X-LOG MGR]
	# 2023-10-11
	# gssg - smalldatetime Ÿ�� ó�� ##
	# gssg - ms2o bulk mode ���� ##
	# gssg - ms2ms normal mode ���� ##
	# gssg - o2o bulk tz ���� ##
	# gssg - o2o bulk ltz ���� ##
	# gssg - postgresql Ŀ�ؼ� Ÿ�� �ƿ� ���� ##
	# gssg - �����ڵ� �� ó�� ##
	# gssg - ora1400 ��ŵ ó�� ##
	# gssg - function �Ķ���� ������ ##
	[ gssg - LG���� MS2O ]


	1.0.24-001 [X-LOG MGR]
	# 2024-02-01
	# gssg -  SELECT ���� ����ڰ� ������ �� �״�� ���� ��� ���� #
	# gssg - linkMode ���� ##
	# gssg - alias ���� ##
	[ gssg - SK��Ʈ�� O2O ]


	1.0.24-002 [X-LOG MGR]
	# 2024-02-13
	# gssg - raw_to_varchar2 ��� ���� #
	[ gssg - ����� O2O ]
	
	
	1.0.24-003 [X-LOG MGR]
	# 2024-03-12
	# gssg - oracle fileThread ����  #
	# gssg - bigType ����ó�� #
	[ gssg - �ѱ����� �ҽ� ����Ŭ�� ���� ]


	1.0.24-004 [X-LOG MGR]
	# 2024-03-26
	[ gssg - ����� rawTovarchar2 ���� ]


	1.0.24-005 [X-LOG MGR]
	# 2024-04-12
	# gssg - �����Ϸ��� O2MS #
	[ gssg - �Ϻ� ��Ʈ��ũ �л� ó�� ]
	
	
################################################		
####### 2024_V01_INIT START
###############################################

	1.0.24-001 [X-LOG INIT]
	# 2024-07-01
	# ayzn - XLInit ��� ����  #
		*���� IDL XL_JOBQ ���̺� ���� -> CDC īŻ�α� ���̺� ������ ����
		*���� IDL Polling ��� -> ��ȸ�� �������� ����
		
	# ayzn - XLInit ��� ���� - conf ���� : CDC CATALOG �������� ���� #
	# ayzn - XLInit ��� ���� - conf ���� : ����� ��� true�� ���� #
	# ayzn - XLInit ��� ���� - conf ���� : �ɼ� ó�� �߰� #
	# ayzn - XLInit ��� ���� - conf ���� : �α׸� ���� ���� #
	
	# ayzn - XLInit ��� ����  - InitThread : �޾ƿ��� �ɼ� �� ����(��å��, �׷��, ���̺��) #
	# ayzn - XLInit ��� ����  - InitThread : ������ �߰�  # 
	# ayzn - XLInit ��� ����  - InitThread : ����  polling ����� while �ּ�ó�� # 
	# ayzn - XLInit ��� ����  - InitThread : CNT���� �޸� �ּ�  # 
	# ayzn - XLInit ��� ����  - InitThread : IDL�� JOBQ ���̺�ó�� �ּ� #  
	# ayzn - XLInit ��� ����  - InitThread : 1. �ҽ� DB ���� ���� #  
	# ayzn - XLInit ��� ����  - InitThread : JOBQ -> SOURCE DB ���� ����  # 
	# ayzn - XLInit ��� ����  - InitThread :  size < 1 üũ �߰� #  
	# ayzn - XLInit ��� ����  - InitThread : JOBQ -> SOURCE DB ���� ���� �� HT_JOBQ_DBMS_RCNT_TMP �ּ� #  
	# ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ�  �ڵ� �ּ� #  
	# ayzn - XLInit ��� ����  - InitThread : �Լ��� runPol�� ���� �� continue���� �ý�������� ����  # 
	# ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ�  JOPQ ���� �ڵ� �ּ� #  
	# ayzn - XLInit ��� ����  - InitThread : source db������  �ʿ��� ���� ���� #  
	# ayzn - XLInit ��� ����  - InitThread : condWhere���� �ּ�ó�� #  
	# ayzn - XLInit ��� ����  - InitThread : ���ڰ� ���� #  
	# ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ�  cnt�޸� ���� �ڵ� �ּ� #  
	# ayzn - XLInit ��� ����  - InitThread : jobseq�����ϰ� ó�� #  
	# ayzn - XLInit ��� ����  - InitThread : INIT���� ������� �ʴ� �ڵ� �ּ� #  
	
	# ayzn - XLInit ��� ����  - DB ���� ���� : jobq, cond commit �ּ� #
	# ayzn - XLInit ��� ����  - DB ���� ���� : jobseq ���� #
	# ayzn - XLInit ��� ����  - DB ���� ���� : report, condition, jobq ���̺� ���� ó�� �ּ� #
	
	# ayzn - XLInit ��� ����  - DBManager : CDCīŻ�α� conf �������� ���� #
	# ayzn - XLInit ��� ����  - DBManager : getDbmsInfo�Լ� ���� CDC īŸ�α� ���̺� ������ ���� #
	# ayzn - XLInit ��� ����  - DBManager : getPolInfo �Լ� �߰� ( Source, Target(�ϴ�� ��å�� ��  NR_POL��  DBMS_SEQ ����) ���� ���� ) #
	# ayzn - XLInit ��� ����  - DBManager : getSourceInfo �Լ� �߰� ( SOURCE ���� ���� ) #
	# ayzn - XLInit ��� ����  - DBManager : getJobRunPolInfo �Լ�  ���� ( CDCīŻ�α� �����Ͽ� �÷����� ���� ) #
	
	# ayzn - XLInit ��� ����  - XLJobRunPol : jobseq �ּ� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : �ɼ� grpcode, tableName �߰� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : source ���̺��̸� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : init���� ��������ʴ� file������� �ּ� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : commit_count, parallel �ɼ� ó�� �߰� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : ���� ��� ���� ���� (Source, Target) #
	# ayzn - XLInit ��� ����  - XLJobRunPol : getJobRunPolInfo �Լ�ó�� �� ������ �߰� �� ���� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : CDC īŻ�α� ���̺� ���� ������ �ּ�ó�� #
	# ayzn - XLInit ��� ����  - XLJobRunPol : Table �÷� ���� ���� �� CDC īŻ�α� ���̺� ������ ���� #
									
	[ ayzn - IDL �ڵ� ������� CDC ���̺�  �ʱ�ȭ ��� ����]

**/

	public static String VERSION 	= "1.0.24-001 [X-LOG INIT]";
	public static String BUILD 		= "2024-07-01";
	
}
