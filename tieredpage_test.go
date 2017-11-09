package pages

import (
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// TestWriteReadPageEntryPageEntry
func TestWriteReadPageEntryPageEntry(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(pt)
	}

	// Allocate a page
	pp, err := pt.pm.allocatePage()
	if err != nil {
		t.Fatal(err)
	}

	offset := int64(0)
	usedBytes := int64(100)
	pageOff := int64(4096)
	if err := writeTieredPageEntry(pp, offset, usedBytes, pageOff); err != nil {
		t.Errorf("Failed to write entry: %v", err)
	}

	readUsedBytes, readPageOff, err := readEntryPageEntry(pp, offset)
	if err != nil {
		t.Errorf("Failed to read entry: %v", err)
	}
	if readUsedBytes != usedBytes {
		t.Errorf("Failed to read usedBytes. Expected %v but was %v", usedBytes, readUsedBytes)
	}
	if readPageOff != pageOff {
		t.Errorf("Failed to read pageOff. Expected %v but was %v", pageOff, readPageOff)
	}

}

// TestAddPage tests if entryPage addPages works as expected
func TestAddPage(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(pt)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Write more than numPageEntries page to the entry to force an extension
	// of the tree
	bytesWritten := int(numPageEntries*pageSize + 1)
	if _, err := entry.Write(fastrand.Bytes(bytesWritten)); err != nil {
		t.Errorf("Failed to write the data to disk: %v", err)
	}

	// Get the physical page on which the entryPage is stored
	pp := entry.ep.pp

	// Check if usedBytes and pageOff of the first entry are set correctly
	usedBytes, pageOff, err := readEntryPageEntry(pp, 0)
	if err != nil {
		t.Errorf("Failed to read entry: %v", err)
	}
	if usedBytes != numPageEntries*pageSize {
		t.Errorf("UsedBytes has wrong value. Expected %v, but was %v",
			numPageEntries*pageSize, usedBytes)
	}
	expectedOff := entry.ep.root.childTables[0].pp.fileOff
	if pageOff != expectedOff {
		t.Errorf("pageOff has wrong value. Expected %v, but was %v", expectedOff, pageOff)
	}

	// Check if usedBytes and pageOff of the second entry are set correctly
	usedBytes, pageOff, err = readEntryPageEntry(pp, 1)
	if err != nil {
		t.Errorf("Failed to read entry: %v", err)
	}
	if usedBytes != int64(bytesWritten) {
		t.Errorf("UsedBytes has wrong value. Expected %v, but was %v",
			bytesWritten, usedBytes)
	}
	expectedOff = entry.ep.root.pp.fileOff
	if pageOff != expectedOff {
		t.Errorf("pageOff has wrong value. Expected %v, but was %v", expectedOff, pageOff)
	}
}

// TestDefrag tests the functionality of the tieredPage's defrag call
func TestDefrag(t *testing.T) {
	// Get a paging tester
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Write data to the entry
	numPages := int(numPageEntries + 1)
	_, err = entry.Write(fastrand.Bytes(numPages * pageSize))
	if err != nil {
		t.Errorf("Failed to write data: %v", err)
	}

	// Truncate data
	if err := entry.Truncate(0); err != nil {
		t.Errorf("Failed to truncate data: %v", err)
	}

	// The tree should only consist of the root node now
	if len(entry.ep.root.childPages) != 0 || len(entry.ep.root.childTables) != 0 {
		t.Errorf("Too many nodes. childPages %v childTables %v",
			len(entry.ep.root.childPages), len(entry.ep.root.childTables))
	}

	if entry.ep.usedSize != 0 {
		t.Errorf("Used size should be %v but was %v", 0, entry.ep.usedSize)
	}

	if len(entry.ep.pages) != 0 {
		t.Errorf("Len pages should be %v but was %v", 0, len(entry.ep.pages))
	}

	// There should be numPages + 2 (for the pagetables) free pages now
	if len(pt.pm.freePages.pages) != numPages+2 {
		t.Logf("expected free pages %v but was %v",
			numPageEntries+2, len(pt.pm.freePages.pages))
	}
}

// TestFreePage checks if getting free pages from the recyclingPage works as expected.
func TestFreePage(t *testing.T) {
	// Get a paging tester
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Write numPageEntries + 1 pages to the entry
	numPages := int(numPageEntries + 1)
	if _, err := entry.Write(fastrand.Bytes(numPages * pageSize)); err != nil {
		t.Errorf("Failed to write pages: %v", err)
	}

	// Free all the pages again
	if err := entry.Truncate(0); err != nil {
		t.Errorf("Failed to truncate entry: %v", err)
	}

	freePages := entry.pm.freePages
	var expectedPage *physicalPage
	for len(freePages.pagesToFree)+len(freePages.pages) > 0 {
		// If buffer is not empty we expect a buffered page
		if len(freePages.pagesToFree) > 0 {
			expectedPage = freePages.pagesToFree[len(freePages.pagesToFree)-1]
		} else {
			expectedPage = freePages.pages[len(freePages.pages)-1]
		}
		newPage, err := entry.pm.freePages.freePage()
		if err != nil {
			t.Fatalf("Failed to free page: %v", err)
		}
		if expectedPage != newPage {
			t.Errorf("Returned page didn't match expected page")
		}
	}

	// Check if all free pages are used
	if len(freePages.pagesToFree)+len(freePages.pages) != 0 {
		t.Errorf("there should be no more free pages")
	}
}

// TestInsertPage tests the funtionality of the pageTable's InsertPage call
func TestInsertPage(t *testing.T) {
	// Get a paging tester
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Insert numPageEntries + 1 pages into the entry
	for i := 0; i < numPageEntries+1; i++ {
		pp, err := pt.pm.allocatePage()
		if err != nil {
			t.Errorf("Failed to allocate page: %v", err)
		}
		if err := entry.ep.insertPage(uint64(i), pp); err != nil {
			t.Errorf("Inserting page failed: %v", err)
		}
	}

	// The table should be the root, have height 1 and numPageEntries height, 0 tables
	table := entry.ep.root
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
