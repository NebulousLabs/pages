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
	if err := writeEntryPageEntry(pp, offset, usedBytes, pageOff); err != nil {
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
