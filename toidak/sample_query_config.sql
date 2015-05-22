drop external table config_tb;
drop table config2_tb;
drop table config3_tb;
drop table config_mtb;
drop table config_kv_tb;


create external table config_tb (line text) location ('pxf://localhost:50070/iLake/*.txt?Profile=HdfsTextMulti') FORMAT 'TEXT' SEGMENT REJECT LIMIT 100000;
create table config2_tb (no serial, line text) distributed randomly;
create table config3_tb (hostname text, outputfilename text, config_cat text, detail text) distributed randomly;
create table config_kv_tb (no int, key text, value text) distributed randomly;
create table config_mtb (no int,key text, value text) distributed randomly;

insert into config2_tb (line) select * from config_tb;
insert into config_mtb select no, substring (line,'#.*?:'),substring(line,'(?!# .+?:).*') from config2_tb
where line ~* '# date :'
   or line ~* '# Hostname :'
   or line ~* '# output filename :'
;

insert into config_kv_tb select no, trim(': ' from substring(line,E'\\S.*?:')),trim(': ' from substring(line,':.*')) from config2_tb where line !~'^#' and line ~':' and line !~ E'^\\d{2}/\\d{2}/\\d{4}';

/*
select substr(key,1,60), substr(value,1,40), count(*) from config_kv_tb group by key,value order by count(*) desc;
*/

select substr(key,1,60), substr(value,1,40), count(*) from config_kv_tb
where key ~'netNetqueueEnabled'
   or key ~'netPktHeapMaxSize'
   or key ~'Current failovermode setting'
group by key,value order by count(*) desc
;
copy (select substr(key,1,60), substr(value,1,40), count(*) from config_kv_tb                                  where key ~'netNetqueueEnabled'
      or key ~'netPktHeapMaxSize'
      or key ~'Current failovermode setting'
group by key,value order by count(*) desc)
to '/home/gpadmin/query_config.csv'
with CSV
;
