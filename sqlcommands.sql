create extension lsm;
create table t(k bigint primary key,p bigint);
create index lsm_index on t using lsm(k);


insert into t values(1,0);
insert into t values(2,0);
insert into t values(3,0);
insert into t values(4,0);
insert into t values(5,0);
insert into t values(6,0);
insert into t values(7,0);
insert into t values(8,0);
insert into t values(9,0);
insert into t values(10,0);
insert into t values(11,0);
insert into t values(12,0);

