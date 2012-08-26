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

// Return this error to indicate a walker should stop iterating.
var StopIeration error = couchError(C.COUCHSTORE_ERROR_CANCEL)

// Walker function.
//
// Stops at the end of the DB or on error.
type WalkFun func(db *Couchstore, di DocInfo) error

// Walker function that also includes the document.
type DocWalkFun func(db *Couchstore, di DocInfo, Doc Document) error

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
	err := maybeError(C.couchstore_open_db(C.CString(pathname),
		flags, &rv.db))
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
func (db *Couchstore) Save(doc Document, docInfo DocInfo) error {
	return maybeError(C.couchstore_save_document(db.db,
		&doc.doc, &docInfo.info, C.COMPRESS_DOC_BODIES))
}

// Get a new document instance with the given id and value.
func NewDocument(id, value string) Document {
	doc := Document{}

	doc.doc.id.buf = C.CString(id)
	doc.doc.id.size = _Ctype_size_t(len(id))
	doc.doc.data.buf = C.CString(value)
	doc.doc.data.size = _Ctype_size_t(len(value))

	return doc
}

// Get the ID of this document
func (doc Document) ID() string {
	return C.GoStringN(doc.doc.id.buf, _Ctype_int(doc.doc.id.size))
}

// Get the value of this document.
func (doc Document) Value() string {
	return C.GoStringN(doc.doc.data.buf, _Ctype_int(doc.doc.data.size))
}

// Create a new docinfo.
func NewDocInfo(id string, meta uint8) DocInfo {
	info := DocInfo{}
	C.initDocInfo(&info.info)

	info.info.id.buf = C.CString(id)
	info.info.id.size = _Ctype_size_t(len(id))

	info.info.content_meta = _Ctype_couchstore_content_meta_flags(meta)

	return info
}

// Get the ID of this document info
func (info DocInfo) ID() string {
	return C.GoStringN(info.info.id.buf, _Ctype_int(info.info.id.size))
}

func freeDocInfo(info *DocInfo) {
	C.couchstore_free_docinfo(info.ptr)
}

func freeDoc(doc *Document) {
	C.couchstore_free_document(doc.ptr)
}

func (db *Couchstore) getDocInfo(id string) (DocInfo, error) {
	var inf *C.DocInfo
	err := maybeError(C.couchstore_docinfo_by_id(db.db,
		unsafe.Pointer(C.CString(id)), _Ctype_size_t(len(id)), &inf))
	if err == nil {
		rv := &DocInfo{*inf, inf}
		runtime.SetFinalizer(rv, freeDocInfo)
		return *rv, nil
	}
	return DocInfo{}, err
}

func (db *Couchstore) GetFromDocInfo(info DocInfo) (Document, error) {
	var doc *C.Doc
	rv := &Document{}

	err := maybeError(C.couchstore_open_doc_with_docinfo(db.db,
		&info.info, &doc, 0))
	if err == nil {
		rv.doc = *doc
		rv.ptr = doc
		runtime.SetFinalizer(rv, freeDoc)
	}
	return *rv, err
}

// Retrieve a document.
func (db *Couchstore) Get(id string) (Document, DocInfo, error) {
	di, err := db.getDocInfo(id)
	if err != nil {
		return Document{}, di, err
	}

	doc, err := db.GetFromDocInfo(di)

	return doc, di, err
}

// Delete a document.
func (db *Couchstore) Delete(id string) error {
	di := NewDocInfo(id, 0)
	di.info.deleted = 1
	return db.Save(NewDocument(id, ""), di)
}

//export callbackAdapt
func callbackAdapt(dbp unsafe.Pointer, infop unsafe.Pointer, ctx unsafe.Pointer) int {
	cb := (*WalkFun)(ctx)
	db := Couchstore{(*C.Db)(dbp), true}
	info := DocInfo{*(*C.DocInfo)(infop), nil}
	switch i := (*cb)(&db, info).(type) {
	case nil:
		return 0
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
	return db.Walk(startkey, func(fdb *Couchstore, di DocInfo) error {
		doc, err := fdb.GetFromDocInfo(di)
		if err != nil {
			return err
		}
		return callback(fdb, di, doc)
	})
}

// Interface for writing bulk data into couchstore.
type BulkWriter interface {
	// Set a document.
	Set(DocInfo, Document)
	// Delete a document.
	Delete(DocInfo)
	// Commit the current batch.
	Commit() error
	// Shut down this bulk interface.
	Close() error
}

type instr struct {
	di  DocInfo
	doc Document
}

type bulkWriter struct {
	update chan instr
	quit   chan bool
	commit chan chan error
}

func (b *bulkWriter) Close() error {
	b.quit <- true
	return nil
}

func (b *bulkWriter) Commit() error {
	ch := make(chan error)
	b.commit <- ch
	return <-ch
}

func (b *bulkWriter) Set(di DocInfo, doc Document) {
	b.update <- instr{di, doc}
}

func (b *bulkWriter) Delete(di DocInfo) {
	b.update <- instr{di, NewDocument("", "")}
}

func (db *Couchstore) commitBulk(batch []instr) error {
	bulk := C.allocate_bulk_docs(_Ctype_size_t(len(batch)))
	for i := range batch {
		C.append_bulk_item(bulk, &batch[i].di.info, &batch[i].doc.doc)
	}
	defer C.free_bulk_docs(bulk)

	err := maybeError(C.execute_batch(db.db, bulk))
	if err != nil {
		return err
	}

	return db.Commit()
}

// Get a bulk writer.
//
// You must call Close() on the bulk writer when you're done bulk
// writing.
func (db *Couchstore) Bulk() BulkWriter {
	rv := &bulkWriter{
		make(chan instr),
		make(chan bool),
		make(chan chan error),
	}

	go func() {
		ever := true
		batch := make([]instr, 0, 100)
		for ever {
			select {
			case <-rv.quit:
				ever = false
			case req := <-rv.commit:
				req <- db.commitBulk(batch)
				batch = batch[:0]
			case i := <-rv.update:
				batch = append(batch, i)
			}
		}
	}()

	return rv
}
