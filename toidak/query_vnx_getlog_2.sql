drop external table vnx_getlog_tb;
drop table vnx_getlog_all_tb;
drop table vnx_getlog_all_bytime_tb;

create external table vnx_getlog_tb (line text) location ('pxf://localhost:50070/data/ps_datalake/performance/SP/IDCF/20150408_VNX_Array/*getlog.txt?Profile=HdfsTextMulti') FORMAT 'TEXT' SEGMENT REJECT LIMIT 1000;

create table vnx_getlog_all_tb (
    daytime timestamp,
    component text,
    event_code text, 
    event_cat text, 
    detail text
) distributed randomly;

insert into vnx_getlog_all_tb
    select 
        to_timestamp(substring(line,E'\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2}'),'MM/DD/YYYY HH24:MI:SS') ,
        regexp_replace(substring(substr(line,20,40),E'.+?\\(.*\\)'),E'\\(.*\\)',''),
        substring(line,E'\\(.*?\\)'),
        substring(line,E'\\(.*?\\).+?[\\.:\\]]'),
        line 
    from vnx_getlog_tb;

select substr(event_cat,0,50), count(*) from vnx_getlog_all_tb group by event_cat order by count(*) desc limit 30;

select substr(event_cat,0,40), count(*) from vnx_getlog_all_tb where detail ~*'(error|fail)' group by event_cat order by count(*) desc limit 30;

select substr(event_cat,0,40),date_trunc('day',daytime), count(*) from vnx_getlog_all_tb  where event_cat ~* '(error|fail)' group by event_cat,date_trunc('day',daytime) order by count(*) desc limit 30 ;

create table vnx_getlog_all_bytime_tb (daytime timestamp, component text, err_cat text, err_count int) distributed randomly;

insert into vnx_getlog_all_bytime_tb with tmp as (select err_cat as err_cat ,component as component ,extract('year' from daytime)||'/'||extract('doy' from daytime) || ' ' || extract('hour' from daytime) || ':' || trunc(extract('minute' from daytime)/10 )*10 as daytime from vnx_getlog_all_tb ) select to_timestamp(daytime,'YYYY/DDD HH24:MI'), err_cat, component, count(*) from tmp group by daytime, component, err_cat order by daytime;

select daytime, sum(case err_cat when ' (7117000f)iSCSI Login Failure   Initiator Data:  IP=10.' then err_count end) as iSCSILoginFailure, sum(case err_cat when ' (6004)NTP Time Synchronization Failed.' then err_count end) as NTPTimeSynch, sum(case err_cat when ' (71170015)iSCSI failure messages are suppressed at Host specific Threshold   Initiator Data:  IP=1' then err_count end)as iSCSIfailureMessageAreSuppressedIP1,sum(case err_cat when ' (71660400)Relocation completed for Storage Pool 0.' then err_count end) as RelocationCompleted,sum(case err_cat when ' (71170015)iSCSI failure messages are suppressed at Global Threshold   Initiator Data:  IP=10.' then err_count end) as iSCSIfalureMessageAreSuppressedIP10 from vnx_getlog_all_bytime_tb group by daytime order by daytime;
