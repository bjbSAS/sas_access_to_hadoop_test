/* Run passthrough SQL via the Hive Connection */
options sastrace=',,,ds' sastraceloc=saslog nostsuffix;

/* Hadoop namenode and where hive is running                                  */
%let myhive2server="sascdh01.race.sas.com";

/* hadoop jarpath 
   - this directory is on the SAS foundation/workspace machine 
     where this code is running and the required Hadoop jars are found        
     could be close to 100 jars in this directory */
     
%let jarpath="/opt/sas/thirdparty/Hadoop_Jars/CDH582";

/* for those that keep the Hadoop config files in another location, separate  */
/* from the jar files.  This would be core-site.xml, yarn-site.xml, etc.      */

%let cfgpath="/opt/sas/thirdparty/Hadoop_Conf/CDH582";
/* %let xmlcfg="/aft/sfw/sas/hadoop_cdh/conf/hadoop_config.xml"; */

/* hive user and password.  this must be a valid combination for the cluster  */
%let hiveuser=hadoop;   /* only used for pathing purposes with kerberos */
%let hivepass=hadoop;   /* <- NOT used with kerberos tickets    */
%let hiveschema=default; 

option set=SAS_HADOOP_JAR_PATH=&jarpath;
option set=SAS_HADOOP_CONFIG_PATH=&cfgpath;

/* proc hadoop options=cfg username="sasdemo" password="Orion123" verbose; */
proc hadoop username="hadoop" password="hadoop" verbose;
	hdfs delete="/tmp/sas_directory";
	hdfs mkdir="/tmp/sas_directory";
	hdfs copyfromlocal="/home/sasdemo/impl.log" 
		out="/tmp/sas_directory/sasdemo/impl.log" overwrite;
run;


libname cdh_hdp hadoop
        server=&myhive2server
        user=&hiveuser                              
        password=&hivepass                          
        database=&hiveschema
        subprotocol=hive2;


proc options option=sqlgeneration;
run; 
options SQLGENERATION = (NONE DBMS='Hadoop');
 
proc delete data=cdh_hdp.cars cdh_hdp.cars_mazda cdh_hdp.cars_test_ds2; run;

data cdh_hdp.cars;
	set sashelp.cars;
run;

/*****************************************************************/
/*PROC DS2 Test */
/*****************************************************************/
/* options ds2accel=any; */
/*  */
/* proc ds2 ; */
/* 	thread compute3; */
/* 	dcl double total; */
/* 	method run(); */
/* 		set cdh_hdp.cars; */
/* 		total + weight; */
/* 	end; */
/* 	endthread; */
/* 	data cdh_hdp.cars_test_ds2; */
/* 		dcl thread compute3 t; */
/* 		method run(); */
/* 			set from t; */
/* 		end; */
/* 	enddata; */
/* 	run; */
/* quit; */

proc sql;
create table cdh_hdp.cars_mazda as select * from cdh_hdp.cars where make="Mazda";
quit;

proc freq data=cdh_hdp.cars_mazda; run;
