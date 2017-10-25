package pages

import (
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// TestInsertPage tests the funtionality of the pageTable's InsertPage call
func TestInsertPage(t *testing.T) {
	// Get a paging tester
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Create pageTable
	table, err := newPageTable(pt.pm)
	if err != nil {
		t.Fatal(err)
	}

	// Insert numPageEntries + 1 pages into the entry
	for i := 0; i < numPageEntries+1; i++ {
		pp, err := pt.pm.allocatePage()
		if err != nil {
			t.Errorf("Failed to allocate page: %v", err)
		}
		table, err = table.insertPage(uint64(i), pp, pt.pm)
		if err != nil {
			t.Errorf("Inserting page failed: %v", err)
		}
	}

	// The table should be the root, have height 1 and numPageEntries height, 0 tables
	if table.parent != nil {
		t.Error("root table should be root but has a parent")
	}
	if table.height != 1 {
		t.Errorf("root table height should be %v but was %v", 1, table.height)
	}
	if len(table.childTables) != 2 {
		t.Errorf("root table should have %v elements in childTables but has %v",
			numPageEntries, len(table.childTables))
	}
	if len(table.childPages) != 0 {
		t.Errorf("root table should have %v elements in childPages but has %v",
			0, len(table.childPages))
	}
	for i, childTable := range table.childTables {
		if childTable.height != 0 {
			t.Errorf("childTable%v's height is % but should be %v", i, childTable.height, 0)
		}
		if childTable.parent == nil {
			t.Errorf("childTable%v has no parent", i)
		}
	}
}

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
	if len(entry.ep.root.childPages) != numPages {
		t.Errorf("PageTable should contain %v pages but has %v",
			numPages, len(entry.ep.root.childPages))
	}

	// Marshal the underlying root table
	data, err := entry.ep.root.Marshal()
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
