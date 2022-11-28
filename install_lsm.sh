#!/bin/bash

export POSTGRES_INSTALLDIR="/home/abhinav/git/data"
export POSTGRES_SRCDIR="/home/abhinav/git/postgresql"
export PGDATA=${POSTGRES_INSTALLDIR}/data

export PG_CONFIG=${POSTGRES_INSTALLDIR}/bin/pg_config

export LD_LIBRARY_PATH=${POSTGRES_INSTALLDIR}/lib:${LD_LIBRARY_PATH}
export PATH=${POSTGRES_INSTALLDIR}/bin:${PATH}

make USE_PGXS=1
make install USE_PGXS=1

export PATH=${POSTGRES_INSTALLDIR}/bin:${PATH}

export PGDATA=${POSTGRES_INSTALLDIR}/data

#initdb -D ${PGDATA}

pg_ctl -D $PGDATA -l logfile stop

pg_ctl -D $PGDATA -l logfile start

