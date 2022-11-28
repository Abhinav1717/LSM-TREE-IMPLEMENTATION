MODULE_big = lsm
OBJS = lsm.o
PGFILEDESC = "lsm tree index based on existing btree index"
#MODULES = lsm.o
EXTENSION = lsm
DATA = lsm--1.0.sql

#REGRESS = test
#REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/lsm3/lsm3.conf



# postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

