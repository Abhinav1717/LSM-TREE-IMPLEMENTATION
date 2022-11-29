/* CS631 start */
#include "access/nbtree.h"
#define MAX_TREES_PER_LEVEL 2
typedef struct
{
	Oid l0;
	Oid l1[MAX_TREES_PER_LEVEL];   /* Oid of base index */

	Oid l2;
	Oid heap;   /* Oid of indexed relation */

	uint64  l0_n_inserts; /* Number of performed inserts since database open  */
	Oid     db_id;    /* user ID (for background worker) */
	Oid     user_id;  /* database Id (for background worker) */
	int     l0_max_index_size; /* Size of top index */
	int 	l1_max_index_size; // Max Size of each tree at L1
	uint64 l1_n_inserts[MAX_TREES_PER_LEVEL];
	int current_l1_tree;
} LsmMetaData;

void
index_rebuild(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo);

void
lsm_create_l1_if_not_exits(
	Relation heap,
	Relation index,
	LsmMetaData* lsmMetaCopy);
void lsmbuildempty( Relation index);
IndexBuildResult * lsmbuild(Relation heap, Relation index, IndexInfo *indexInfo);

static bool lsm_insert(Relation rel, Datum *values, bool *isnull,
			ItemPointer ht_ctid, Relation heapRel,
			IndexUniqueCheck checkUnique,
			IndexInfo *indexInfo);



#define LSMPageGetMeta(p) \
	((LsmMetaData *)(((BTMetaPageData *) PageGetContents(p))+1))


/* CS631 end */
