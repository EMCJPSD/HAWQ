drop external table messages_tb;
drop table messages_err_tb;
drop table messages_err_bytime_tb;

create external table messages_tb (line text) location ('pxf://localhost:50070/iLake/*messages.log?Profile=HdfsTextMulti') FORMAT 'TEXT' SEGMENT REJECT LIMIT 100;
create table messages_err_tb (daytime timestamp,err_cat text, detail text) distributed randomly;
insert into messages_err_tb select to_timestamp(substring(line,E'\\S{3}\\s\\d{2}\\s\\d{2}:\\d{2}:\\d{2}'),'Mon DD HH24:MI:SS') ,regexp_replace(substring(substr(line,17),E'\\S*'),E'\\d+',' ') ,line from messages_tb where line ~*'error' or line ~*'fail';
select err_cat, count(*) from messages_err_tb group by err_cat order by count(*) desc;

create table messages_err_bytime_tb (time text, err_cat text, err_count int) distributed randomly;
insert into messages_err_bytime_tb with tmp as (select err_cat as err_cat ,extract('hour' from daytime) || ':' || trunc(extract('minute' from daytime)/10 )*10 as time from messages_err_tb ) select time::time, err_cat, count(*) from tmp group by time, err_cat order by time;

select time, sum(case err_cat when 'sfcb-lsi_storage[ ]:' then err_count end) as sfcb, sum(case err_cat when 'stbyname' then err_count end) as stbyname, sum(case err_cat when 'vmkernel:' then err_count end)as vmkernel,sum(case err_cat when 'iscsid:' then err_count end) as iscsid from messages_err_bytime_tb group by time order by time;

