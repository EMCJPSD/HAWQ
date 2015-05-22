drop external table esx_kernelwarning_je01vp04avs10_tb;
drop table esx_kernelwarning_je01vp04avs10err_tb;
drop table esx_kernelwarning_je01vp04avs10err_bytime_tb;

create external table esx_kernelwarning_je01vp04avs10_tb (line text) location ('pxf://localhost:50070/data/ps_datalake/performance/SP/IDCF/20150331_ESXi/esx-je01v-p04avs10.shamrock.local-2015-03-30--04.59/vmkwarning.log?Profile=HdfsTextMulti') FORMAT 'TEXT' SEGMENT REJECT LIMIT 1000;
create table esx_kernelwarning_je01vp04avs10err_tb (daytime timestamp,err_cat text, detail text) distributed randomly;

insert into esx_kernelwarning_je01vp04avs10err_tb select to_timestamp(substring(line,E'\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}'),'YYYY-MM-DD HH24:MI:SS') ,regexp_replace(substring(substr(line,25),E'\\).+?:.+?:'),E'\\)',''),substr(line,0,50) from esx_kernelwarning_je01vp04avs10_tb where line ~*'error' or line ~*'fail';
select err_cat, count(*) from esx_kernelwarning_je01vp04avs10err_tb group by err_cat order by count(*) desc;

create table esx_kernelwarning_je01vp04avs10err_bytime_tb (daytime timestamp, err_cat text, err_count int) distributed randomly;
insert into esx_kernelwarning_je01vp04avs10err_bytime_tb with tmp as (select err_cat as err_cat ,extract('year' from daytime)||'/'||extract('doy' from daytime) || ' ' || extract('hour' from daytime) || ':' || trunc(extract('minute' from daytime)/10 )*10 as daytime from esx_kernelwarning_je01vp04avs10err_tb ) select to_timestamp(daytime,'YYYY/DDD HH24:MI'), err_cat, count(*) from tmp group by daytime, err_cat order by daytime;

select daytime, sum(case err_cat when 'ALERT: PowerPath:' then err_count end) as ALERT_PowerPath,
 sum(case err_cat when 'WARNING: ScsiPath:' then err_count end) as WARNING_ScsiPath,
 sum(case err_cat when 'WARNING: PCI:' then err_count end)as WARNING_PCI,
 sum(case err_cat when 'WARNING: SwapFileOps:' then err_count end)as WARNING_SwapFileOps,
 sum(case err_cat when 'WARNING: ScsiScan:' then err_count end)as WARNING_ScsiScan,
 sum(case err_cat when 'WARNING: LinNet:' then err_count end)as WARNING_LinNet,
 sum(case err_cat when 'WARNING: HBX:' then err_count end) as WARNING_HBX from esx_kernelwarning_je01vp04avs10err_bytime_tb group by daytime order by daytime;
