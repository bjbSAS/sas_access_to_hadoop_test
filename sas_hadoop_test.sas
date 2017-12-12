options sastrace=',,,d' sastraceloc=saslog nostsuffix;

OPTIONS SET=SAS_HADOOP_JAR_PATH="/opt/sas/thirdparty/Hadoop_Jars/CDH582";
OPTIONS SET=SAS_HADOOP_CONFIG_PATH="/opt/sas/thirdparty/Hadoop_Conf/CDH582"; 

LIBNAME cdh_hdp HADOOP  PORT=10000 SERVER="sascdh01.race.sas.com"
				user=hadoop password=hadoop ;
				

/* create a table */
proc sql;
connect to hadoop(PORT=10000 SERVER="sascdh01.race.sas.com"   USER="hadoop"  PASSWORD="hadoop"); 
exec( drop table cars_prc) by hadoop;
exec( create table cars_prc (make string,  model string,  msrp double) ) by hadoop; 
quit;

/* load data and query from hive */
proc sql; 
  insert into cdh_hdp.cars_prc 
  select make, model, msrp  
  from sashelp.cars ;   
quit;

proc sql; 
select * from cdh_hdp.cars_prc; 
quit;

/* create a new table from existing: observe log with and without DBMAX_TEXT=128  */
proc sql ;
   connect to HADOOP (SERVER="sascdh01.race.sas.com" PORT=10000 user=hadoop password=hadoop DBMAX_TEXT=128); 
   drop table cdh_hdp.DEPT_CDH;
   create table cdh_hdp.DEPT_CDH as select 
      dept_id length = 8 label = 'dept_id',
      dept_name length = 30 label = 'dept_name'
   from connection to HADOOP
   (
      select * from department
   );
   disconnect from HADOOP; 
quit;



/* explore FILENAME connectivity */
FILENAME hdpfile1 hadoop "/user/hadoop/gutenberg/pg20417.txt" user='hadoop' ; 
DATA _NULL_; 
INFILE hdpfile1 ; 
INPUT; 
LIST; 
RUN;
