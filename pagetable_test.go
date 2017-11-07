package pages

import (
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// TestMarshalLoad tests if marshalling a pageTable, writing it to disk and
// loading it works as expected
func TestMarshalLoad(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Write numPages pages worth of data to the entry
	numPages := 10
	_, err = entry.Write(fastrand.Bytes(numPages * pageSize))
	if err != nil {
		t.Error("Failed to write data to the entry")
	}
	if totalPages(entry.ep.root) != uint64(numPages) {
		t.Errorf("PageTable should contain %v pages but has %v",
			numPages, totalPages(entry.ep.root))
	}

	// Marshal the underlying root table
	data, err := entry.ep.root.marshal()
	if err != nil {
		t.Errorf("Failed to marshal pageTable: %v", err)
	}

	// Unmarshal the data and compare
	entries, err := unmarshalPageTable(data)
	if err != nil {
		t.Errorf("Failed to unmarshal pageTable: %v", err)
	}
	if len(entries) != len(entry.ep.root.childPages) {
		t.Errorf("wrong length. expected %v but was %v",
			len(entry.ep.root.childTables), len(entries))
	}
	for i, offset := range entries {
		if offset != entry.ep.root.childPages[uint64(i)].fileOff {
			t.Errorf("offset should have been %v but was %v",
				entry.ep.root.childPages[uint64(i)].fileOff, offset)
		}
	}
}
