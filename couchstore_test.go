package couchstore

import (
	"fmt"
	"os"
	"reflect"
	"runtime"
	"testing"
)

const testFilename = ",test-database.couch"

func TestOpenFailure(t *testing.T) {
	db, err := Open(testFilename, false)
	if err == nil {
		t.Fatalf("Expected error opening non-existent database")
	}
	if db != nil {
		t.Fatalf("Got a database!")
	}
}

func TestOpenSuccess(t *testing.T) {
	db, err := Open(",test-database.couch", true)
	if err != nil {
		t.Fatalf("Error creating database:  %v", err)
	}
	defer db.Close()
	defer os.Remove(testFilename)
}

func TestDocumentMutation(t *testing.T) {
	db, err := Open(",test-database.couch", true)
	if err != nil {
		t.Fatalf("Error creating database:  %v", err)
	}
	defer db.Close()
	defer os.Remove(testFilename)

	err = db.Set(NewDocInfo("x", 0), NewDocument("x", "value of x"))
	if err != nil {
		t.Fatalf("Error saving new document:  %v", err)
	}

	if err = db.Commit(); err != nil {
		t.Fatalf("Error committing change: %v", err)
	}

	doc, di, err := db.Get("x")
	if err != nil {
		t.Fatalf("Error loading stored document: %v", err)
	}

	if di.ID() != "x" {
		t.Fatalf("Expected info id 'x', got %#v", di.ID())
	}
	if doc.Value() != "value of x" {
		t.Fatalf("Expected doc value 'value of x', got %#v", doc)
	}
	if doc.ID() != "x" {
		t.Fatalf("Expected doc id 'x', got %#v", doc)
	}

	err = db.Delete("x")
	if err != nil {
		t.Fatalf("Error deleting document: %v", err)
	}

	doc2, di2, err := db.Get("x")
	if err != nil {
		t.Fatalf("Expected error getting deleted doc, got %#v/%#v",
			di2, doc2)
	}

	runtime.GC()
}

func TestWalking(t *testing.T) {
	data := map[string]string{
		"a": "aye",
		"b": "bye",
		"c": "cya",
		"d": "dye",
	}
	db, err := Open(",test-database.couch", true)
	if err != nil {
		t.Fatalf("Error creating database:  %v", err)
	}
	defer db.Close()
	defer os.Remove(testFilename)

	for k, v := range data {
		err = db.Set(NewDocInfo(k, 0), NewDocument(k, v))
		if err != nil {
			t.Fatalf("Error saving new document:  %v", err)
		}
	}

	db.Commit()

	found := []string{}
	expect := []string{"a", "b", "c", "d"}

	err = db.Walk("", func(fdb *Couchstore, di DocInfo) error {
		found = append(found, di.ID())
		return nil
	})
	if err != nil {
		t.Fatalf("Error walking: %v", err)
	}

	if !reflect.DeepEqual(found, expect) {
		t.Fatalf("Expected %#v, got %#v", found, expect)
	}

	found = []string{}
	expect = []string{"b", "c"}

	err = db.Walk("b", func(fdb *Couchstore, di DocInfo) error {
		found = append(found, di.ID())
		if di.ID() >= "c" {
			return StopIteration
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Error walking: %v", err)
	}

	if !reflect.DeepEqual(found, expect) {
		t.Fatalf("Expected %#v, got %#v", found, expect)
	}
}

func TestDocWalking(t *testing.T) {
	data := map[string]string{
		"a": "aye",
		"b": "bye",
		"c": "cya",
		"d": "dye",
	}
	db, err := Open(",test-database.couch", true)
	if err != nil {
		t.Fatalf("Error creating database:  %v", err)
	}
	defer db.Close()
	defer os.Remove(testFilename)

	for k, v := range data {
		err = db.Set(NewDocInfo(k, 0), NewDocument(k, v))
		if err != nil {
			t.Fatalf("Error saving new document:  %v", err)
		}
	}

	db.Commit()

	found := map[string]string{}
	expect := data

	err = db.WalkDocs("", func(fdb *Couchstore, di DocInfo, doc Document) error {
		found[di.ID()] = doc.Value()
		return nil
	})
	if err != nil {
		t.Fatalf("Error walking: %v", err)
	}

	if !reflect.DeepEqual(found, expect) {
		t.Fatalf("Expected %#v, got %#v", found, expect)
	}

	found = map[string]string{}
	expect = map[string]string{"b": "bye", "c": "cya"}

	err = db.WalkDocs("b", func(fdb *Couchstore, di DocInfo, doc Document) error {
		found[di.ID()] = doc.Value()
		if di.ID() >= "c" {
			return StopIteration
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Error walking: %v", err)
	}

	if !reflect.DeepEqual(found, expect) {
		t.Fatalf("Expected %#v, got %#v", found, expect)
	}
}

func TestBulkWriter(t *testing.T) {
	db, err := Open(",test-database.couch", true)
	if err != nil {
		t.Fatalf("Error creating database:  %v", err)
	}
	defer db.Close()
	defer os.Remove(testFilename)

	db.Set(NewDocInfo("deleteme", 0), NewDocument("deleteme", "val"))
	db.Commit()

	bw := db.Bulk()
	defer bw.Close()

	stuff := map[string]string{}

	for i := 0; i < 13; i++ {
		stuff[fmt.Sprintf("k%d", i)] = fmt.Sprintf("Value %d", i)
	}

	for k, v := range stuff {
		bw.Set(NewDocInfo(k, 0), NewDocument(k, v))
	}
	bw.Delete(NewDocInfo("deleteme", 0))

	err = bw.Commit()
	if err != nil {
		t.Fatalf("Error storing batch: %v", err)
	}

	found := map[string]string{}

	err = db.WalkDocs("", func(fdb *Couchstore, di DocInfo, doc Document) error {
		if !di.IsDeleted() {
			found[di.ID()] = doc.Value()
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Error walking: %v", err)
	}

	if !reflect.DeepEqual(found, stuff) {
		t.Fatalf("Expected\n%#v\ngot\n%#v", found, stuff)
	}
}
