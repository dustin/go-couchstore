// Couchstore API for go.
package couchstore

/*
#cgo LDFLAGS: -lcouchstore

#include "csgo.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

type Couchstore struct {
	db     *C.Db
	isOpen bool
}

type couchError int

type Document struct {
	doc C.Doc
	ptr *C.Doc
}

type DocInfo struct {
	info C.DocInfo
	ptr  *C.DocInfo
}

func (e couchError) Error() string {
	return C.GoString(C.couchstore_strerror(_Ctype_couchstore_error_t(e)))
}

const DocIsCompressed = C.COUCH_DOC_IS_COMPRESSED

func maybeError(e _Ctype_couchstore_error_t) error {
	if e != C.COUCHSTORE_SUCCESS {
		return couchError(e)
	}
	return nil
}

// Open a database.
func Open(pathname string, create bool) (*Couchstore, error) {
	rv := &Couchstore{}
	flags := _Ctype_couchstore_open_flags(0)
	if create {
		flags = C.COUCHSTORE_OPEN_FLAG_CREATE
	}
	cstr := C.CString(pathname)
	defer C.cfree(cstr)
	err := maybeError(C.couchstore_open_db(cstr, flags, &rv.db))
	if err == nil {
		rv.isOpen = true
	} else {
		rv = nil
	}

	return rv, err
}

// Close the database.
func (db *Couchstore) Close() error {
	if db == nil || !db.isOpen {
		return nil
	}

	return maybeError(C.couchstore_close_db(db.db))
}

// Commit pending data.
func (db *Couchstore) Commit() error {
	return maybeError(C.couchstore_commit(db.db))
}

// Store a document.
func (db *Couchstore) Set(docInfo *DocInfo, doc *Document) error {
	return maybeError(C.couchstore_save_document(db.db,
		&doc.doc, &docInfo.info, C.COMPRESS_DOC_BODIES))
}

// Get a new document instance with the given id and value.
func NewDocument(id, value string) *Document {
	doc := &Document{}

	doc.doc.id.buf = C.CString(id)
	doc.doc.id.size = _Ctype_size_t(len(id))
	doc.doc.data.buf = C.CString(value)
	doc.doc.data.size = _Ctype_size_t(len(value))

	runtime.SetFinalizer(doc, freeMyDoc)

	return doc
}

// Get the ID of this document
func (doc *Document) ID() string {
	return C.GoStringN(doc.doc.id.buf, _Ctype_int(doc.doc.id.size))
}

// Get the value of this document.
func (doc *Document) Value() string {
	return C.GoStringN(doc.doc.data.buf, _Ctype_int(doc.doc.data.size))
}

// Create a new docinfo.
func NewDocInfo(id string, meta uint8) *DocInfo {
	info := &DocInfo{}
	C.initDocInfo(&info.info)

	info.info.id.buf = C.CString(id)
	info.info.id.size = _Ctype_size_t(len(id))

	info.info.content_meta = _Ctype_couchstore_content_meta_flags(meta)

	runtime.SetFinalizer(info, freeMyDocInfo)

	return info
}

// Get the ID of this document info
func (info *DocInfo) ID() string {
	return C.GoStringN(info.info.id.buf, _Ctype_int(info.info.id.size))
}

// True if this docinfo represents a deleted document.
func (info DocInfo) IsDeleted() bool {
	return info.info.deleted != 0
}

// Free docinfo made from go.
func freeMyDocInfo(info *DocInfo) {
	C.cfree(info.info.id.buf)
}

// Free doc made from go.
func freeMyDoc(doc *Document) {
	C.cfree(doc.doc.id.buf)
	C.cfree(doc.doc.data.buf)
}

// Free docinfo made from couchstore
func freeDocInfo(info *DocInfo) {
	C.couchstore_free_docinfo(info.ptr)
}

// Free doc made from couchstore
func freeDoc(doc *Document) {
	C.couchstore_free_document(doc.ptr)
}

func (db *Couchstore) getDocInfo(id string) (*DocInfo, error) {
	var inf *C.DocInfo
	idstr := C.CString(id)
	defer C.cfree(idstr)
	err := maybeError(C.couchstore_docinfo_by_id(db.db,
		unsafe.Pointer(idstr), _Ctype_size_t(len(id)), &inf))
	if err == nil {
		rv := &DocInfo{*inf, inf}
		runtime.SetFinalizer(rv, freeDocInfo)
		return rv, nil
	}
	return &DocInfo{}, err
}

func (db *Couchstore) GetFromDocInfo(info *DocInfo) (*Document, error) {
	var doc *C.Doc
	rv := &Document{}

	err := maybeError(C.couchstore_open_doc_with_docinfo(db.db,
		&info.info, &doc, C.DECOMPRESS_DOC_BODIES))
	if err == nil {
		rv.doc = *doc
		rv.ptr = doc
		runtime.SetFinalizer(rv, freeDoc)
	}
	return rv, err
}

// Retrieve a document.
func (db *Couchstore) Get(id string) (*Document, *DocInfo, error) {
	di, err := db.getDocInfo(id)
	if err != nil {
		return nil, di, err
	}

	doc, err := db.GetFromDocInfo(di)

	return doc, di, err
}

// Delete a document.
func (db *Couchstore) Delete(id string) error {
	di := NewDocInfo(id, 0)
	di.info.deleted = 1
	return db.Set(di, NewDocument(id, ""))
}
