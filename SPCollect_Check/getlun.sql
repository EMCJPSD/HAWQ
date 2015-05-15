create external table getlun (col1 text) location ('pxf://localhost:50070/wangzz/emcGrab/clariion/10.133.239.10/naviseccli_getlun.txt?Profile=HdfsTextMulti') FORMAT 'TEXT';

create table luninfo (lunid int,luncapacity bigint);
create table temp1 (id serial,lunid int);
create table temp2 (id serial, luncapacity);

insert into temp1 (lunid) select cast(substring(col1,20) as int) from getlun where col1 like 'LOGICAL UNIT NUMBER%';
insert into temp2 (luncapacity) select cast(trim(substring(col1,26)) as bigint) from getlun where col1 like 'LUN Capacity(M%';

insert into LunInfo select lunid,luncapacity from temp1 inner join temp2 on temp1.id=temp2.id;