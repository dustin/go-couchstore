#ifndef CSGO_H
#define CSGO_H 1

#include <libcouchstore/couch_db.h>

typedef struct {
    size_t cap;
    size_t len;
    DocInfo **infos;
    Doc **docs;
} couchstore_bulk_t;

void cfree(char *p);

void initDocInfo(DocInfo *info);
couchstore_error_t start_all_docs(Db *db, const char *start, void *ctx);

couchstore_bulk_t *allocate_bulk_docs(size_t howmany);
void free_bulk_docs(couchstore_bulk_t *bulk);
void append_bulk_item(couchstore_bulk_t *bulk, DocInfo *docInfo, Doc *doc);
couchstore_error_t execute_batch(Db *db, couchstore_bulk_t *bulk);

#endif /* CSGO_H */
