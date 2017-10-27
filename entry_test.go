package pages

import (
	"bytes"
	"io"
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// TestEntrySeek tests the functionality of the Entry's seek call
func TestEntrySeek(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Entry should have no pages
	if len(entry.pages) != 0 {
		t.Errorf("Entry should have 0 pages but has %v", len(entry.pages))
	}

	// Seeking before the file start shouldn't work
	pos, err := entry.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("Seeking before start shouldn't work")
	}
	if pos != 0 {
		t.Errorf("Position should still be 0 but was %v", pos)
	}

	// Add a page to the entry
	pp, err := pt.pm.managedAllocatePage()
	if err != nil {
		t.Errorf("Failed to allocate new page: %v", err)
	}
	entry.pages = append(entry.pages, pp)

	// Seek to the start of the page
	pos, err = entry.Seek(0, io.SeekStart)
	if err != nil {
		t.Errorf("Failed to seek start of file: %v", err)
	}
	if pos != 0 {
		t.Errorf("Position should be 0 but was %v", pos)
	}
	if entry.cursorOff != 0 || entry.cursorPage != 0 {
		t.Errorf("cursorOff/cursorPage should be %v/%v but were %v/%v",
			0, 0, entry.cursorOff, entry.cursorPage)
	}

	// Seek to the end of the page
	pos, err = entry.Seek(0, io.SeekEnd)
	if err != nil {
		t.Logf("Failed to seek end of file: %v", err)
	}
	if pos != pageSize {
		t.Logf("Position should be %v but was %v", pageSize, pos)
	}
	if entry.cursorOff != 0 || entry.cursorPage != 1 {
		t.Errorf("cursorOff/cursorPage should be %v/%v but were %v/%v",
			0, 1, entry.cursorOff, entry.cursorPage)
	}

	// Add two more pages to the entry
	pp1, err := pt.pm.managedAllocatePage()
	if err != nil {
		t.Errorf("Failed to allocate new page: %v", err)
	}
	pp2, err := pt.pm.managedAllocatePage()
	if err != nil {
		t.Errorf("Failed to allocate new page: %v", err)
	}
	entry.pages = append(entry.pages, pp1, pp2)

	// Seek to the end of the 3 pages
	pos, err = entry.Seek(0, io.SeekEnd)
	if err != nil {
		t.Errorf("Failed to seek end of file: %v", err)
	}
	if pos != 3*pageSize {
		t.Errorf("Position should be %v but was %v", 3*pageSize, pos)
	}
	if entry.cursorOff != 0 || entry.cursorPage != 3 {
		t.Errorf("cursorOff/cursorPage should be %v/%v but were %v/%v",
			0, 3, entry.cursorOff, entry.cursorPage)
	}

	// Seek 6000 to the left
	off := int64(-6000)
	pos, err = entry.Seek(off, io.SeekCurrent)
	if err != nil {
		t.Errorf("Failed to seek: %v", err)
	}
	if pos != 3*pageSize+off {
		t.Errorf("Position should be %v but was %v", 3*pageSize+off, pos)
	}
	if entry.cursorOff != 2*pageSize+off || entry.cursorPage != 1 {
		t.Errorf("cursorOff/cursorPage should be %v/%v but were %v/%v",
			2*pageSize+off, 1, entry.cursorOff, entry.cursorPage)
	}

	// And 2000 back to the right
	off2 := int64(2000)
	pos, err = entry.Seek(off2, io.SeekCurrent)
	if err != nil {
		t.Errorf("Failed to seek: %v", err)
	}
	if pos != 3*pageSize+off+off2 {
		t.Errorf("Position should be %v but was %v", 3*pageSize+off+off2, pos)
	}
	if entry.cursorOff != pageSize+off+off2 || entry.cursorPage != 2 {
		t.Errorf("cursorOff/cursorPage should be %v/%v but were %v/%v",
			pageSize+off+off2, 2, entry.cursorOff, entry.cursorPage)
	}
}

// TestEntryRead tests the functionality of the Entry's Read and ReadAt calls
func TestEntryRead(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Add 3 pages
	entryData := make([]byte, 0)
	for i := 0; i < 3; i++ {
		// Allocate pages
		pp, err := pt.pm.managedAllocatePage()
		if err != nil {
			t.Errorf("Failed to allocate new page: %v", err)
		}
		entry.pages = append(entry.pages, pp)

		// Write data to them and remember the data
		pageData := fastrand.Bytes(pageSize)
		if _, err := pp.writeAt(pageData, 0); err != nil {
			t.Errorf("Failed to write data to new page: %v", err)
		}
		entryData = append(entryData, pageData...)
	}

	// Read all of the data and compare it to the written data
	readData := make([]byte, len(entryData))
	if _, err := entry.Read(readData); err != nil {
		t.Errorf("Couldn't read the written data: %v", err)
	}
	if bytes.Compare(entryData, readData) != 0 {
		t.Error("Read data didn't match the written data")
	}

	// Read from the middle of the Entry
	offset := int64(4000)
	if _, err := entry.Seek(offset, io.SeekStart); err != nil {
		t.Errorf("Seeking to position %v failed", offset)
	}
	if _, err := entry.Read(readData); err != nil {
		t.Errorf("Couldn't read the data from offset %v", offset)
	}
	if bytes.Compare(entryData[offset:], readData[0:int64(len(entryData))-offset]) != 0 {
		t.Error("Read data didn't match the written data")
	}

	// We read to the end. Reading again should return EOF
	if _, err := entry.Read(readData); err != io.EOF {
		t.Errorf("Error should have been %v but was %v", io.EOF, offset)
	}

	// ReadAt should still work without moving the cursor
	offset = 0
	cursorPage := entry.cursorPage
	cursorOff := entry.cursorOff
	if _, err := entry.ReadAt(readData, offset); err != nil {
		t.Errorf("Couldn't read the data from offset %v", err)
	}
	if bytes.Compare(entryData, readData) != 0 {
		t.Error("Read data didn't match the written data")
	}
	if entry.cursorPage != cursorPage || entry.cursorOff != cursorOff {
		t.Errorf("Cursor position was moved during ReadAt. Was %v/%v but should be %v/%v",
			entry.cursorPage, entry.cursorOff, cursorPage, cursorOff)
	}
}

// TestEntryWrite tests the funtionality of the Entry's Write and WriteAt calls
func TestEntryWrite(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// The entry is supposed to have 0 pages
	if len(entry.pages) != 0 {
		t.Errorf("Entry is supposed to have 0 pages initially but had %v", len(entry.pages))
	}

	// Write a few times the number of pageSize to the entry
	pages := 10000
	entryData := fastrand.Bytes(pages * pageSize)
	n, err := entry.Write(entryData)
	if n != pages*pageSize || err != nil {
		t.Errorf("%v bytes were written to the page: %v", n, err)
	}

	// Check the number of pages in the Entry
	if len(entry.pages) != pages {
		t.Errorf("Entry was supposed to have %v pages but had %v", pages, len(entry.pages))
	}

	// Read the data to check if it was written correctly
	offset := int64(0)
	readData := make([]byte, len(entryData))
	if _, err := entry.ReadAt(readData, offset); err != nil {
		t.Errorf("Couldn't read the data from offset %v", err)
	}
	if bytes.Compare(entryData, readData) != 0 {
		t.Error("Read data didn't match the written data")
	}

	/// WriteAt should still work without moving the cursor
	offset = 4000
	cursorPage := entry.cursorPage
	cursorOff := entry.cursorOff
	if _, err := entry.WriteAt(entryData, offset); err != nil {
		t.Errorf("Couldn't write the data: %v", err)
	}
	if _, err := entry.ReadAt(readData, offset); err != nil {
		t.Errorf("Couldn't read the data from offset %v", err)
	}
	n = len(entryData) - int(offset)
	if bytes.Compare(entryData[0:n], readData[0:n]) != 0 {
		t.Error("Read data didn't match the written data")
		t.Log(len(entryData[offset:]))
		t.Log(len(readData[0 : int64(len(entryData))-offset]))
	}
	if entry.cursorPage != cursorPage || entry.cursorOff != cursorOff {
		t.Errorf("Cursor position was moved during WriteAt. Was %v/%v but should be %v/%v",
			entry.cursorPage, entry.cursorOff, cursorPage, cursorOff)
	}
}

// TestTruncate tests the functionality of the Entry's Truncate method
func TestTruncate(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Get a new entry
	entry, _, err := pt.pm.Create()
	if err != nil {
		t.Fatal(err)
	}

	// Write a few times the number of pageSize to the entry
	pages := 10000
	entryData := fastrand.Bytes(pages * pageSize)
	n, err := entry.Write(entryData)
	if n != pages*pageSize || err != nil {
		t.Errorf("%v bytes were written to the page: %v", n, err)
	}

	// The used size should be pages * pageSize
	if entry.ep.usedSize != int64(pages*pageSize) {
		t.Errorf("usedSize should be %v but was %v", pages*pageSize, entry.ep.usedSize)
	}

	// Truncate the file
	truncatedSize := int64(15000)
	if err := entry.Truncate(truncatedSize); err != nil {
		t.Errorf("Truncate failed %v", err)
	}

	// Make sure the usedSize was adjusted
	if entry.ep.usedSize != truncatedSize {
		t.Errorf("usedSize should be %v but was %v", truncatedSize, entry.ep.usedSize)
	}

	// Check if the number of remaining pages in the entry is ok
	expectedPages := truncatedSize/pageSize + 1
	if int64(len(entry.pages)) != expectedPages {
		t.Errorf("len(entry.pages) should be %v but was %v", expectedPages, len(entry.pages))
	}

	// The remaining pages should be in the freePages slice
	freedPageTables := int64(pages / numPageEntries)
	if int64(len(pt.pm.freePages)) != int64(pages)-expectedPages+freedPageTables {
		t.Errorf("there should be %v free pages but there are %v",
			int64(pages)-expectedPages+freedPageTables, len(pt.pm.freePages))
	}

	// Make sure the data wasn't corrupted
	readData := make([]byte, truncatedSize)
	if _, err := entry.Seek(0, io.SeekStart); err != nil {
		t.Errorf("Failed to seek the start of the truncated data: %v", err)
	}
	if _, err := entry.Read(readData); err != nil {
		t.Errorf("Failed to read truncated data: %v", err)
	}
	if bytes.Compare(entryData[:truncatedSize], readData) != 0 {
		t.Error("Data is corrupted after truncating it")
	}

	// The next read should fail with EOF
	if _, err := entry.Read(readData); err != io.EOF {
		t.Errorf("Read didn't fail with EOF: %v", err)
	}
}
