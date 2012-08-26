package couchstore

/*
#include "csgo.h"
*/
import "C"

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
	di.info.deleted = 1
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
