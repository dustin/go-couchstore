package couchstore

import (
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

	err = db.Save(NewDocument("x", "value of x"), NewDocInfo("x", 0))
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
		err = db.Save(NewDocument(k, v), NewDocInfo(k, 0))
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
			return StopIeration
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
		err = db.Save(NewDocument(k, v), NewDocInfo(k, 0))
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
			return StopIeration
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
