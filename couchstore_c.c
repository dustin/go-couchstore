#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sysexits.h>
#include <stdlib.h>
#include <unistd.h>

#include "csgo.h"

#include "_cgo_export.h"

void initDocInfo(DocInfo *info) {
    memset(info, 0x00, sizeof(DocInfo));
}

static int walk_callback(Db *db, DocInfo *docInfo, void *ctx) {
    return callbackAdapt(db, docInfo, ctx);
}

couchstore_error_t start_all_docs(Db *db, const char *start, void *ctx) {
    sized_buf sb;
    sb.buf = (char*)start;
    sb.size = strlen(start);
    return couchstore_all_docs(db, &sb, 0, walk_callback, ctx);
}

couchstore_bulk_t *allocate_bulk_docs(size_t howmany) {
    couchstore_bulk_t *rv = calloc(1, sizeof(couchstore_bulk_t));
    rv->infos = calloc(howmany, sizeof(DocInfo*));
    rv->docs = calloc(howmany, sizeof(Doc*));
    rv->len = 0;
    rv->cap = (rv->infos && rv->docs) ? howmany : 0;
    return rv;
}

void free_bulk_docs(couchstore_bulk_t *bulk) {
    free(bulk->infos);
    free(bulk->docs);
    memset(bulk, 0x00, sizeof(couchstore_bulk_t));
}

void append_bulk_item(couchstore_bulk_t *b, DocInfo *docInfo, Doc *doc) {
    const int incr = 8;
    int i;
    if (b->len >= b->cap) {
        void *p = realloc(b->infos, b->cap + incr);
        if (p == NULL) {
            return;
        }
        b->infos = p;

        p = realloc(b->docs, b->cap + incr);
        if (p == NULL) {
            return;
        }
        b->docs = p;

        b->cap += incr;
    }

    b->infos[b->len] = docInfo;
    b->docs[b->len] = doc;

    b->len++;
}

couchstore_error_t execute_batch(Db *db, couchstore_bulk_t *bulk) {
    return couchstore_save_documents(db, bulk->docs, bulk->infos,
                                     bulk->len, COMPRESS_DOC_BODIES);
}

void freecstring(char *p) {
    free(p);
}
