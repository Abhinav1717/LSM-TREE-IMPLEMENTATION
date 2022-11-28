drop extension lsm cascade;create extension lsm;create table t(k bigint,p bigint);create index lsm_index on t using lsm(k);

