package pages

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/NebulousLabs/Sia/build"
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
