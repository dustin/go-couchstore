package couchstore

/*
#include "csgo.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// Return this error to indicate a walker should stop iterating.
var StopIteration error = couchError(C.COUCHSTORE_ERROR_CANCEL)

// Walker function.
//
// Stops at the end of the DB or on error.
type WalkFun func(db *Couchstore, di *DocInfo) error

// Walker function that also includes the document.
type DocWalkFun func(db *Couchstore, di *DocInfo, doc *Document) error

//export callbackAdapt
func callbackAdapt(dbp unsafe.Pointer, infopg unsafe.Pointer, ctx unsafe.Pointer) int {
	cb := (*WalkFun)(ctx)
	db := Couchstore{(*C.Db)(dbp), true}
	infop := (*C.DocInfo)(infopg)
	info := &DocInfo{*infop, infop}
	switch i := (*cb)(&db, info).(type) {
	case nil:
		runtime.SetFinalizer(info, freeDocInfo)
		return 1
	case couchError:
		return int(i)
	}
	// Really need couchstore to give us a better error here.
	return -404
}

// Walk the DB from a specific location.
func (db *Couchstore) Walk(startkey string, callback WalkFun) error {
	e := C.start_all_docs(db.db,
		C.CString(startkey),
		unsafe.Pointer(&callback))
	if e != C.COUCHSTORE_ERROR_CANCEL && e != C.COUCHSTORE_SUCCESS {
		return couchError(e)
	}
	return nil
}

// Walk the DB from a specific location including the complete docs.
func (db *Couchstore) WalkDocs(startkey string, callback DocWalkFun) error {
	return db.Walk(startkey, func(fdb *Couchstore, di *DocInfo) error {
		doc, err := fdb.GetFromDocInfo(di)
		if err != nil {
			return err
		}
		return callback(fdb, di, doc)
	})
}
