package couchstore

import (
	"os"
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
