package pages

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/NebulousLabs/Sia/build"
	"github.com/NebulousLabs/fastrand"
)

// pagingTester is a helper object to simplify testing
type pagingTester struct {
	pm *PageManager
}

// Close is a helper function for a clean pagingTester shutdown
func (pt pagingTester) Close() error {
	return pt.pm.Close()
}

// newPagingTester returns a ready-to-rock pagingTester
func newPagingTester(name string) (*pagingTester, error) {
	// Create temp dir
	testdir := build.TempDir("paging", name)
	err := os.MkdirAll(testdir, 0700)
	if err != nil {
		return nil, err
	}

	dataFilePath := filepath.Join(testdir, "data.dat")
	pm, err := New(dataFilePath)
	if err != nil {
		return nil, err
	}

	return &pagingTester{
		pm: pm,
	}, nil
}

func TestAllocatePage(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Allocate numPages pages
	numPages := 5
	pages := make([]*physicalPage, numPages)
	for i := 0; i < numPages; i++ {
		page, err := pt.pm.managedAllocatePage()
		if err != nil {
			t.Errorf("Failed to allocate page number %v: %v", i, err)
		}
		pages[i] = page
	}

	// Get file stats
	stats, err := pt.pm.file.Stat()
	if err != nil {
		t.Fatalf("Failed to get file stats: %v", err)
	}

	// Check filesize afterwards
	if stats.Size() != int64(numPages*pageSize+dataOff) {
		t.Errorf("Filesize should be %v, but was %v", numPages*pageSize+dataOff, stats.Size())
	}

	// Check if pages were allocated
	if pt.pm.allocatedPages != int64(numPages) {
		t.Errorf("AllocatedPages has wrong value. Should be %v, but was %v",
			numPages, pt.pm.allocatedPages)
	}

	// Check if fields were set correctly
	for i := 0; i < numPages; i++ {
		if pages[i].fileOff != int64(i*pageSize+dataOff) {
			t.Errorf("Page %v has wrong offset. Was %v, but should be %v",
				i, pages[i].fileOff, i*pageSize+dataOff)
		}
	}
}

// TestRecovery tests if the data is still available after closing the
// pagemanager and reloading it
func TestRecovery(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	entry, identifier, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Write numPages pages worth of data to the entry
	numPages := 10
	data := fastrand.Bytes(numPages * pageSize)
	_, err = entry.Write(data)
	if err != nil {
		t.Error("Failed to write data to the entry")
	}

	// The root table should contain numPages children
	if len(entry.ep.root.childPages) != numPages {
		t.Errorf("Entry should have %v children but had %v", numPages, len(entry.ep.root.childPages))
	}

	// Open the entry again
	entry, err = pt.pm.Open(identifier)
	if err != nil {
		t.Errorf("Failed to open the entery: %v", err)
	}

	// Check if the entry contains the right number of pages
	if len(entry.pages) != numPages {
		t.Errorf("entry should contain %v pages but only had %v", numPages, len(entry.pages))
	}

	// Read the previously written data and compare it
	readData := make([]byte, len(data))
	if _, err := entry.Read(readData); err != nil {
		t.Errorf("Failed to read data: %v", err)
		t.Logf("numPages %v", len(entry.pages))
	}
	if bytes.Compare(data, readData) != 0 {
		t.Errorf("Read data doesn't match written data")
	}

	// Check if the length of the entry matches the data's length
	length, err := entry.Seek(0, io.SeekEnd)
	if err != nil {
		t.Error(err)
	}
	if length != int64(len(data)) {
		t.Errorf("length should be %v but was %v", len(data), length)
	}
}
