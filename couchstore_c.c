#include <sys/types.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <sysexits.h>
#include <stdlib.h>
#include <unistd.h>

#include <libcouchstore/couch_db.h>

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
