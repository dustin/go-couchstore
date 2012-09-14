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

	verifyInfo := func(got, exp DBInfo) {
		// Ignore these fields.
		got.SpaceUsed = 0
		exp.SpaceUsed = 0
		got.HeaderPosition = 0
		exp.HeaderPosition = 0
		if !reflect.DeepEqual(got, exp) {
			t.Fatalf("Expected info = %v, got %v", exp, got)
		}
	}

	inf := db.Info()
	verifyInfo(inf, DBInfo{0, 0, 0, 0, 0})

	err = db.Set(NewDocInfo("x", 0), NewDocument("x", []byte("value of x")))
	if err != nil {
		t.Fatalf("Error saving new document:  %v", err)
	}

	if err = db.Commit(); err != nil {
		t.Fatalf("Error committing change: %v", err)
	}

	inf = db.Info()
	verifyInfo(inf, DBInfo{1, 1, 0, 93, 4096})

	doc, di, err := db.Get("x")
	if err != nil {
		t.Fatalf("Error loading stored document: %v", err)
	}

	if di.ID() != "x" {
		t.Fatalf("Expected info id 'x', got %#v", di.ID())
	}
	if !reflect.DeepEqual(doc.Value(), []byte("value of x")) {
		t.Fatalf("Expected doc value 'value of x', got %#v", doc)
	}
	if doc.ID() != "x" {
		t.Fatalf("Expected doc id 'x', got %#v", doc)
	}

	err = db.Delete("x")
	if err != nil {
		t.Fatalf("Error deleting document: %v", err)
	}

	inf = db.Info()
	verifyInfo(inf, DBInfo{2, 0, 1, 83, 4096})

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
		err = db.Set(NewDocInfo(k, 0), NewDocument(k, []byte(v)))
		if err != nil {
			t.Fatalf("Error saving new document:  %v", err)
		}
	}

	db.Commit()

	found := []string{}
	expect := []string{"a", "b", "c", "d"}

	err = db.Walk("", func(fdb *Couchstore, di *DocInfo) error {
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

	err = db.Walk("b", func(fdb *Couchstore, di *DocInfo) error {
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
		err = db.Set(NewDocInfo(k, 0), NewDocument(k, []byte(v)))
		if err != nil {
			t.Fatalf("Error saving new document:  %v", err)
		}
	}

	db.Commit()

	found := map[string]string{}
	expect := data

	err = db.WalkDocs("", func(fdb *Couchstore, di *DocInfo, doc *Document) error {
		found[di.ID()] = string(doc.Value())
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

	err = db.WalkDocs("b", func(fdb *Couchstore, di *DocInfo, doc *Document) error {
		found[di.ID()] = string(doc.Value())
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

	db.Set(NewDocInfo("deleteme", 0), NewDocument("deleteme", []byte("val")))
	db.Commit()

	bw := db.Bulk()
	defer bw.Close()

	stuff := map[string]string{}

	for i := 0; i < 13; i++ {
		stuff[fmt.Sprintf("k%d", i)] = fmt.Sprintf("Value %d", i)
	}

	for k, v := range stuff {
		bw.Set(NewDocInfo(k, 0), NewDocument(k, []byte(v)))
	}
	bw.Delete(NewDocInfo("deleteme", 0))

	err = bw.Commit()
	if err != nil {
		t.Fatalf("Error storing batch: %v", err)
	}

	found := map[string]string{}

	err = db.WalkDocs("", func(fdb *Couchstore, di *DocInfo, doc *Document) error {
		if !di.IsDeleted() {
			found[di.ID()] = string(doc.Value())
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
