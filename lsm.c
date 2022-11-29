
/* CS631 start */

#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "access/nbtree.h"
#include "commands/vacuum.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include "lsm.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/transam.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "bootstrap/bootstrap.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/objectaccess.h"
#include "catalog/partition.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_description.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/storage.h"
#include "commands/event_trigger.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parser.h"
#include "pgstat.h"
#include "rewrite/rewriteManip.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_rusage.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(lsm_handler);

IndexBuildResult * lsm_not_l0_build(Relation heap,Relation index,IndexInfo* indexInfo);

int no_of_tuples_in_l2 = 0;

/* Initialize lsm control data entry */
static void lsm_init_entry(LsmMetaData* entry, Relation index)
{
	entry->l0_n_inserts = 0;

	for(int i = 0 ; i < MAX_TREES_PER_LEVEL; i++){
		entry->l1[i]= InvalidOid;
	}
	entry->l0=index->rd_id;
	entry->l2 = InvalidOid;

	elog(NOTICE,"lsm_init_entry: INIT writing l0 %d",entry->l0);

	entry->heap = index->rd_index->indrelid;
	entry->db_id = MyDatabaseId;
	entry->user_id = GetUserId();
	entry->l0_max_index_size = 2;
	entry->l1_max_index_size = 2*entry->l0_max_index_size;

	for(int i = 0 ; i < MAX_TREES_PER_LEVEL; i++){
			entry->l1_n_inserts[i]= 0;
		}
	entry->current_l1_tree = 0;
	elog(NOTICE,"lsm_init_entry: INIT writing l0 %d",entry->l0_max_index_size);

}

/* Get B-Tree index size (number of blocks) */
static BlockNumber lsm_get_index_size(Oid relid)
{
       Relation index = index_open(relid, AccessShareLock);
       BlockNumber size = RelationGetNumberOfBlocks(index);
	   index_close(index, AccessShareLock);
       return size;
}

//			char* topidxname = psprintf("%s_top%d", relname, i);
/* Truncate top index */
static void
lsm_truncate_index(Relation index, Oid heap_oid)
{
	elog(NOTICE,"truncate heap rel Oid %d",heap_oid);
	//Relation index = index_open(index_oid, AccessExclusiveLock);
	Relation heap = table_open(heap_oid, AccessShareLock); /* heap is actually not used, because we will not load data to top indexes */
	IndexInfo* indexInfo = BuildDummyIndexInfo(index);
	RelationTruncate(index, 0);
	elog(NOTICE, "lsm: truncate index %s", RelationGetRelationName(index));
	/* parameter third is for reindex it called ambuild internally*/
	index_build(heap, index, indexInfo, true, false);
	/* rewrite meta*/ 
	//index_close(index, AccessExclusiveLock);
	table_close(heap, AccessShareLock);
}

static void
lsm_truncate_not_l0_index(Relation index, Oid heap_oid){

	elog(NOTICE,"truncate not l0: heap rel Oid %d",heap_oid);
	//Relation index = index_open(index_oid, AccessExclusiveLock);
	Relation heap = table_open(heap_oid, AccessShareLock); /* heap is actually not used, because we will not load data to top indexes */
//	IndexInfo* indexInfo = BuildDummyIndexInfo(index);
	RelationTruncate(index, 0);
	elog(NOTICE, "truncate not l0: truncate index %s", RelationGetRelationName(index));
	/* parameter third is for reindex it called ambuild internally*/
	lsm_not_l0_build(heap, index, BuildIndexInfo(index));
	/* rewrite meta*/
	//index_close(index, AccessExclusiveLock);
	table_close(heap, AccessShareLock);
}
/* Merge top index into base index */
static void
lsm_merge_indexes(Oid dst_oid, Relation top_index, Oid heap_oid)
{
	elog(NOTICE,"merge dest_oid %d,heap rel Oid %d",dst_oid,heap_oid);
	Relation heap = table_open(heap_oid, AccessShareLock);
	Relation base_index = index_open(dst_oid, RowExclusiveLock);
	IndexScanDesc scan;
	bool ok;
	Oid  save_am = base_index->rd_rel->relam;

	elog(NOTICE, "lsm: merge index %s with size %d blocks", RelationGetRelationName(top_index), RelationGetNumberOfBlocks(top_index));


	//Currently copying the index tuples one by one, should have used bottom up build construction for better node occupancy
	base_index->rd_rel->relam = BTREE_AM_OID;
	scan = index_beginscan(heap, top_index, SnapshotAny, 0, 0);
	scan->xs_want_itup = true;
	btrescan(scan, NULL, 0, 0, 0);
	for (ok = _bt_first(scan, ForwardScanDirection); ok; ok = _bt_next(scan, ForwardScanDirection))
	{

		IndexUniqueCheck checkUnique = UNIQUE_CHECK_NO;
		IndexTuple itup = scan->xs_itup;
		
		//Not being used in our case
		if (BTreeTupleIsPosting(itup))
		{
			/* 
			 * If index tuple is posting item, we need to transfer it to normal index tuple.
			 * Posting list is representing by index tuple with INDEX_ALT_TID_MASK bit set in t_info and
			 * BT_IS_POSTING bit in TID offset, following by array of TIDs.
			 * We need to store right TID (taken from xs_heaptid) and correct index tuple length
			 * (not including size of TIDs array), clearing INDEX_ALT_TID_MASK.
			 * For efficiency reasons let's do it in place, saving and restoring original values after insertion is done.
			 */
			ItemPointerData save_tid = itup->t_tid;
			unsigned short save_info = itup->t_info;
			itup->t_info = (save_info & ~(INDEX_SIZE_MASK | INDEX_ALT_TID_MASK)) + BTreeTupleGetPostingOffset(itup);
			itup->t_tid = scan->xs_heaptid;
			_bt_doinsert(base_index, itup, checkUnique,false, heap); /* lsm index is not unique so need not to heck for duplica
tes */
			itup->t_tid = save_tid;
			itup->t_info = save_info;
		}
		else
		{
			_bt_doinsert(base_index, itup, checkUnique,false, heap);  /* lsm index is not unique so need not to heck for duplica
tes */
		}
	}
	index_endscan(scan);
	base_index->rd_rel->relam = save_am;
	index_close(base_index, RowExclusiveLock);
	table_close(heap, AccessShareLock);
}
/*
 * lsm access methods implementation
 */

void
lsmbuildempty( Relation index)
{
	/*
	This function is copied from nbtree.c and our metdata is added to existing metadata

	*/
	elog(NOTICE,"Build Empty Called");
	Page		metapage;
	BTMetaPageData* metad;
	LsmMetaData* lsmMeta;
	Oid save_am;
	save_am = index->rd_rel->relam;

	/* Construct metapage. */
	metapage = (Page) palloc(BLCKSZ);
	index->rd_rel->relam = BTREE_AM_OID;
	_bt_initmetapage(metapage, P_NONE, 0, _bt_allequalimage(index, false));
	index->rd_rel->relam = save_am;

	metad = BTPageGetMeta(metapage);
	lsmMeta=(LsmMetaData*)(metad+1);

	lsm_init_entry(lsmMeta,index);
	((PageHeader) metapage)->pd_lower =
		((char *) metad + sizeof(BTMetaPageData)) +sizeof(LsmMetaData) - (char *) metapage;
	/*
	 * Write the page and log it.  It might seem that an immediate sync would
	 * be sufficient to guarantee that the file exists on disk, but recovery
	 * itself might remove it while replaying, for example, an
	 * XLOG_DBASE_CREATE or XLOG_TBLSPC_CREATE record.  Therefore, we need
	 * this even when wal_level=minimal.
	 */
	PageSetChecksumInplace(metapage, BTREE_METAPAGE);
	smgrwrite(index->rd_smgr, INIT_FORKNUM, BTREE_METAPAGE,
			  (char *) metapage, true);
	log_newpage(&index->rd_smgr->smgr_rnode.node, INIT_FORKNUM,
				BTREE_METAPAGE, metapage, true);

	/*
	 * An immediate sync is required even if we xlog'd the page, because the
	 * write did not go through shared_buffers and therefore a concurrent
	 * checkpoint may have moved the redo pointer past our xlog record.
	 */
	smgrimmedsync(index->rd_smgr, INIT_FORKNUM);
}
IndexBuildResult *
lsmbuild(Relation heap, Relation index, IndexInfo *indexInfo)
{

	elog(NOTICE,"lsmbuild: function called");
	elog(NOTICE,"lsmbuild: Relation index oid is %d",index);
	Buffer	metabuf;
	Page metapg;
	BTMetaPageData* metad;
	LsmMetaData* lsmMeta;
	IndexBuildResult* result;
	Oid save_am;
	save_am = index->rd_rel->relam;
	index->rd_rel->relam = BTREE_AM_OID;
	elog(NOTICE,"lsmbuild: Starting B-Tree Build");
	result=btbuild( heap, index, indexInfo);
	index->rd_rel->relam = save_am;
	elog(NOTICE,"lsmbuild: btbuild returned; now getting lsm metadata");
	metabuf = _bt_getbuf(index, BTREE_METAPAGE, BT_WRITE);
	metapg = BufferGetPage(metabuf);
	metad = BTPageGetMeta(metapg);
	lsmMeta=(LsmMetaData*)(metad+1);
	lsm_init_entry(lsmMeta,index); //Change lsm_init entry differnet for l0
	elog(NOTICE,"lsmbuild: l0 created, oid %d",lsmMeta->l0);
	lsmMeta->l0_n_inserts=lsmMeta->l0_n_inserts+result->index_tuples;
	elog(NOTICE,"lsmbuild: No of tuple generated by btreebuild %ld ", lsmMeta->l0_n_inserts);
	_bt_relbuf(index, metabuf);

	
		return result;
}


IndexBuildResult * lsm_not_l0_build(Relation heap,Relation index,IndexInfo* indexInfo){
		elog(NOTICE,"lsm_not_l0_build: function called");
		elog(NOTICE,"lsm_not_l0_build: Relation index oid is %d",index);
		Buffer	metabuf;
		Page metapg;
		BTMetaPageData* metad;
		LsmMetaData* lsmMeta;
		IndexBuildResult* result;
		Oid save_am;
		save_am = index->rd_rel->relam;
		index->rd_rel->relam = BTREE_AM_OID;
		elog(NOTICE,"lsm_not_l0_build: Starting B-Tree Build");
		result=btbuild( heap, index, indexInfo);
		index->rd_rel->relam = save_am;
		elog(NOTICE,"lsm_not_l0_build: btbuild returned");
//		metabuf = _bt_getbuf(index, BTREE_METAPAGE, BT_WRITE);
//		metapg = BufferGetPage(metabuf);
//		metad = BTPageGetMeta(metapg);
//		lsmMeta=(LsmMetaData*)(metad+1);
//		lsm_init_entry(lsmMeta,index); //Change lsm_init entry differnet for l0
//		elog(NOTICE,"lsm_l1_build: l0 created, oid %d",lsmMeta->l0);
//		lsmMeta->l0_n_inserts=lsmMeta->l0_n_inserts+result->index_tuples;
//		elog(NOTICE,"lsm_l1_build: No of tuple generated by btreebuild %ld ", lsmMeta->l0_n_inserts);
//		_bt_relbuf(index, metabuf);


			return result;

}
lsm_create_l1_tree_if_not_exits(Relation heap,Relation index,LsmMetaData* lsmMetaCopy){

	char* newName;
	char* l0name;
	Relation l1;
	if(lsmMetaCopy->l1[lsmMetaCopy->current_l1_tree] == InvalidOid){
			l0name=index->rd_rel->relname.data;
			newName=(char*)palloc(NAMEDATALEN+4);
			newName[0]='L';
			newName[1]='1';
			newName[2]='_';
			newName[3]='0'+lsmMetaCopy->current_l1_tree;
//			sprintf(newName[2],"%d",lsmMetaCopy->current_l1_tree);
//			=itoa();
		for(int i=0;i<NAMEDATALEN;++i){
			newName[i+4]=l0name[i];
		}
		elog(NOTICE,"lsm_create_l1_tree_if_not_exits: L1 index name = %s ",newName);
		lsmMetaCopy->l1[lsmMetaCopy->current_l1_tree]= index_concurrently_create_copy(
		   		 heap,/*heap relation*/
		   		 index->rd_id,/*old oid*/
		   		 index->rd_rel->reltablespace, /*old Oid table space*/
		   		 newName/*new name =l1+oldname*/
		   	 ); // Not workinng
		// opening index not table issue
		l1=index_open(lsmMetaCopy->l1[lsmMetaCopy->current_l1_tree],AccessShareLock);


//		index_build(heap,l1,BuildIndexInfo(l1),false,false);
		lsm_not_l0_build(heap,l1,BuildIndexInfo(l1));
		Relation pg_index = table_open(IndexRelationId,RowExclusiveLock);
		HeapTuple indexTuple = SearchSysCacheCopy1(INDEXRELID,ObjectIdGetDatum(lsmMetaCopy->l1[lsmMetaCopy->current_l1_tree]));

//		l1->rd_index->indisvalid=true;
//		Form_pg_index indexForm= l1->rd_index;
		Form_pg_index indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
		indexForm->indisvalid = true;

		CatalogTupleUpdate(pg_index,&indexTuple->t_self,indexTuple);

		table_close(pg_index,RowExclusiveLock);

//		index_set_state_flags(l1->rd_index->indexrelid, INDEX_CREATE_SET_VALID);
		index_close(l1,AccessShareLock);
	}
}
void
lsm_create_l2_if_not_exits(Relation heap,Relation index,LsmMetaData* lsmMetaCopy){
	char* newName;
	char* l0name;
	Relation l2;
	if(lsmMetaCopy->l2==InvalidOid){
			l0name=index->rd_rel->relname.data;
			newName=(char*)palloc(13);
			newName[0]='L';
			newName[1]='2';
			newName[2]='_';
			newName[3]='l';
			newName[4]='s';
			newName[5]='m';
			newName[6]='_';
			newName[7]='i';
			newName[8]='n';
			newName[9]='d';
			newName[10]='e';
			newName[11]='x';
			newName[12]='\0';

//			newName[3]='0'+lsmMetaCopy->current_l1_tree;
		elog(NOTICE,"lsm_create_l2_if_not_exits: L2 index name = %s ",newName);
		lsmMetaCopy->l2= index_concurrently_create_copy(
			heap,/*heap relation*/
			index->rd_id,/*old oid*/
	   		index->rd_rel->reltablespace, /*old Oid table space*/
			newName/*new name =l1+oldname*/
		);

		l2=index_open(lsmMetaCopy->l2,AccessShareLock);
		lsm_not_l0_build(heap,l2,BuildIndexInfo(l2));
//		l2->rd_index->indisvalid=true;
		Relation pg_index = table_open(IndexRelationId,RowExclusiveLock);
		HeapTuple indexTuple = SearchSysCacheCopy1(INDEXRELID,ObjectIdGetDatum(lsmMetaCopy->l2));

//		l1->rd_index->indisvalid=true;
//		Form_pg_index indexForm= l1->rd_index;
		Form_pg_index indexForm = (Form_pg_index)GETSTRUCT(indexTuple);
		indexForm->indisvalid = true;

		CatalogTupleUpdate(pg_index,&indexTuple->t_self,indexTuple);

		table_close(pg_index,RowExclusiveLock);
		//index_set_state_flags(l2->rd_index->indexrelid, INDEX_CREATE_SET_VALID);
		index_close(l2,AccessShareLock);
	}
}
/* Insert in active top index, on overflow take data from lo and initiate merge to base index  l1*/
static bool
lsm_insert(Relation rel, Datum *values, bool *isnull,
			ItemPointer ht_ctid, Relation heapRel,
			IndexUniqueCheck checkUnique,
			IndexInfo *indexInfo)
{
	bool result;
	Buffer	metabuf;
	Page metapg;
	BTMetaPageData* metad;
	LsmMetaData* lsmMeta;
	LsmMetaData* lsmMetaCopy;
	Oid save_am;
	save_am = rel->rd_rel->relam;

	elog(NOTICE,"*****lsm Insert calling btree insert*****");

	rel->rd_rel->relam = BTREE_AM_OID;
	bool indexUnchanged = true;

	result=btinsert( rel,values, isnull,  ht_ctid,  heapRel, checkUnique,indexUnchanged,indexInfo);
	rel->rd_rel->relam = save_am;

	metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
	metapg = BufferGetPage(metabuf);
	
	metad = BTPageGetMeta(metapg);
	lsmMeta=(LsmMetaData*)(metad+1);
	elog(NOTICE,"BT META START %p BT META SIZE %ld LSM META START %p lo from meta %d prev size %ld",metad,sizeof(BTMetaPageData),lsmMeta,lsmMeta->l0,lsmMeta->l0_n_inserts);
	lsmMeta->l0_n_inserts=lsmMeta->l0_n_inserts+1;
	elog(NOTICE,"No of tuple generated by btree build %ld ", lsmMeta->l0_n_inserts);

	if(lsmMeta->l0_n_inserts >= lsmMeta->l0_max_index_size){ // if L0 overflows

//		lsmMetaCopy=(LsmMetaData*)palloc(sizeof(LsmMetaData));
//		elog(NOTICE,"palloc success");
//		memcpy(lsmMetaCopy,lsmMeta,sizeof(LsmMetaData));
//		elog(NOTICE,"memcpy success ");
//		_bt_relbuf(rel, metabuf);
//		elog(NOTICE,"Max no of inserts 64+ achieved Closed relation");
		elog(NOTICE,"before l2: %d %d",lsmMeta->current_l1_tree, MAX_TREES_PER_LEVEL);

		if(lsmMeta->current_l1_tree >= MAX_TREES_PER_LEVEL){ // if all trees in L1 are full // condition need to be checked
			elog(NOTICE, "current l1 tree is equal to max trees per level");
			lsmMetaCopy=(LsmMetaData*)palloc(sizeof(LsmMetaData));
			elog(NOTICE,"palloc success");
			memcpy(lsmMetaCopy,lsmMeta,sizeof(LsmMetaData));
			elog(NOTICE,"memcpy success ");

//			elog(NOTICE,"");
			// l1 is full create l2 if not exist
			elog(NOTICE,"L1 is full. Merging with l2 \n");
//			pgstat_report_activitlsm_truncate_indexy(STATE_RUNNING, "merging");

			for(int i = 0; i < MAX_TREES_PER_LEVEL; i++){ // copying all trees at L1 layer to L2

				//Doubtful about writing rel
				Relation l1_index_relation = index_open(lsmMetaCopy->l1[i],AccessShareLock);
				elog(NOTICE,"creation of l2 if not found");
				lsm_create_l2_if_not_exits(heapRel,l1_index_relation, lsmMetaCopy);
				elog(NOTICE,"l2 oid in meta %d  l1 oid from rel %d l2 oid  %d  merging heap rel oid %d",lsmMetaCopy->l1[i],lsmMetaCopy->l2,rel->rd_id,lsmMetaCopy->heap);
				elog(NOTICE,"merging ");


				lsm_merge_indexes(lsmMetaCopy->l2, l1_index_relation , lsmMetaCopy->heap);

				elog(NOTICE,"merging success in l2");
				elog(NOTICE,"trucncate");
				pgstat_report_activity(STATE_RUNNING, "truncate And rebuild l1 index");
				lsm_truncate_not_l0_index(l1_index_relation,lsmMetaCopy->heap);
				index_close(l1_index_relation, AccessShareLock);

			}

//			metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
			metapg = BufferGetPage(metabuf);
			metad = BTPageGetMeta(metapg);
			lsmMeta=(LsmMetaData*)(metad+1);


			lsmMeta->l1_max_index_size=lsmMetaCopy->l1_max_index_size;

			lsmMeta->l0_max_index_size=lsmMetaCopy->l0_max_index_size;

			lsmMeta->user_id=lsmMetaCopy->user_id;
			lsmMeta->db_id=lsmMetaCopy->db_id;

			for(int i=0;i<lsmMeta->current_l1_tree; i++){
				lsmMeta->l1_n_inserts[i] = 0;
			}
//			if(lsmMeta->l1_n_inserts[lsmMeta->current_l1_tree] >= lsmMeta->l1_max_index_size){
//				lsmMeta->current_l1_tree+=1;
//			}
			lsmMeta->current_l1_tree = 0;

			lsmMeta->l0_n_inserts=lsmMetaCopy->l0_n_inserts;
			lsmMeta->heap=lsmMetaCopy->heap;
			lsmMeta->l2=lsmMetaCopy->l2;

			for(int i=0; i< MAX_TREES_PER_LEVEL; i++){
				lsmMeta->l1[i]=lsmMetaCopy->l1[i];
			}

			lsmMeta->l0=lsmMetaCopy->l0;

//			_bt_relbuf(rel, metabuf);
			pfree(lsmMetaCopy);

		}
		
		//Also Currently the lsm meta data is being stored with the btree meta data of the top level, but could have stored lsm meta data seperately to avoid initializing it 		 	again and again 
		
		//Instead of copying entire tree we could have just changed to Oid of the l1 level and instantiated the l0 level again.
		lsmMetaCopy=(LsmMetaData*)palloc(sizeof(LsmMetaData));
		elog(NOTICE,"palloc success");
		memcpy(lsmMetaCopy,lsmMeta,sizeof(LsmMetaData));
		elog(NOTICE,"memcpy success ");
		_bt_relbuf(rel, metabuf);
		elog(NOTICE,"L0 overflowed, Buffer Lock released");



		elog(NOTICE,"L1 not full. It is currently at %d\n",lsmMetaCopy->current_l1_tree);
		pgstat_report_activity(STATE_RUNNING, "merging");
		elog(NOTICE,"creation of l1_%d if not found", lsmMetaCopy->current_l1_tree);
		lsm_create_l1_tree_if_not_exits(heapRel,rel, lsmMetaCopy);
		elog(NOTICE,"l0 oid in meta %d  l0 oid from rel %d l1 index %d oid  %d  merging heap rel oid %d", lsmMetaCopy->l0,rel->rd_id,lsmMetaCopy->current_l1_tree,lsmMetaCopy->l1[lsmMetaCopy->current_l1_tree],lsmMetaCopy->heap);
		elog(NOTICE,"merging ");
		lsm_merge_indexes(lsmMetaCopy->l1[lsmMetaCopy->current_l1_tree], rel, lsmMetaCopy->heap);
		elog(NOTICE,"merging success");
		elog(NOTICE,"trucncate");
		pgstat_report_activity(STATE_RUNNING, "truncate And rebuild l0");
		lsm_truncate_index(rel,lsmMetaCopy->heap);
		elog(NOTICE,"maintaing new meta");

		metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
		metapg = BufferGetPage(metabuf);
		metad = BTPageGetMeta(metapg);
		lsmMeta=(LsmMetaData*)(metad+1);


		lsmMeta->l1_max_index_size=lsmMetaCopy->l1_max_index_size;

		lsmMeta->l0_max_index_size=lsmMetaCopy->l0_max_index_size;

		lsmMeta->user_id=lsmMetaCopy->user_id;
		lsmMeta->db_id=lsmMetaCopy->db_id;

		for(int i=0; i< MAX_TREES_PER_LEVEL; i++){
					lsmMeta->l1[i]=lsmMetaCopy->l1[i];
					lsmMeta->l1_n_inserts[i] = lsmMetaCopy->l1_n_inserts[i];
				}

		lsmMeta->heap=lsmMetaCopy->heap;
		lsmMeta->l2=lsmMetaCopy->l2;

		lsmMeta->l0=lsmMetaCopy->l0;

		lsmMeta->l0_n_inserts = lsmMetaCopy->l0_n_inserts;

		lsmMeta->current_l1_tree=lsmMetaCopy->current_l1_tree;

		pfree(lsmMetaCopy);

		elog(NOTICE,"before: %d %d %d",lsmMeta->current_l1_tree, lsmMeta->l0_n_inserts, lsmMeta->l1_n_inserts[lsmMeta->current_l1_tree]);

		lsmMeta->l1_n_inserts[lsmMeta->current_l1_tree] += lsmMeta->l0_n_inserts;

		if(lsmMeta->l1_n_inserts[lsmMeta->current_l1_tree] >= lsmMeta->l1_max_index_size){
			lsmMeta->current_l1_tree+=1;
		}
		elog(NOTICE,"after: %d %d %d",lsmMeta->current_l1_tree, lsmMeta->l0_n_inserts, lsmMeta->l1_n_inserts[lsmMeta->current_l1_tree]);

		lsmMeta->l0_n_inserts=0;

		_bt_relbuf(rel, metabuf);

		elog(NOTICE,"insert and merging done");


	}else{
		elog(NOTICE,"Returing from insert in L0 because Size not Exceed");
			_bt_relbuf(rel, metabuf);
	}
	return result;
}



//Scanning not implemented

Datum lsm_handler(PG_FUNCTION_ARGS)
{
//	elog(NOTICE,"LSM HANDLER FUCNCTION CALL");
//	pg_printf("LSM HANDLER PSPRINTF");
	IndexAmRoutine *amroutine;
	amroutine= makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = true;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;   /* We can't check that index is unique without accessing base index */
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false; /* TODO: not sure if it will work correctly with merge */
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false; /* TODO: parallel scac is not supported yet */
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 	VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsmbuild;
	amroutine->ambuildempty = lsmbuildempty;
	amroutine->aminsert = lsm_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = btoptions;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = btbeginscan;
	amroutine->amrescan = btrescan;
	amroutine->amgettuple = btgettuple;
	amroutine->amgetbitmap = btgetbitmap;
	amroutine->amendscan = btendscan;
	amroutine->ammarkpos = NULL;  /*  When do we need index_markpos? Can we live without it? */
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/* CS631 end */
