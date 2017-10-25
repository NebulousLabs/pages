package pages

import (
	"errors"
	"io"

	"github.com/NebulousLabs/Sia/build"
)

type (
	// Entry is a single entry in the database. It implements the
	// ReadWriteSeeker interface to enable easy writes to the file
	Entry struct {
		// pm is a pointer to the PageManager that created this Entry
		pm *PageManager

		// ep is the tiered entryPage for this entry
		ep *entryPage

		// pages is a list of the physical pages that are used to store the
		// data of this entry
		pages []*physicalPage

		// cursorOff is the offset of the cursor from the start of the current
		// page it is pointed at
		cursorOff int64

		// cursorPage is the index of the page in pages to which the cursor points
		cursorPage int64
	}
)

// Sync calls sync on the underlying file of the Page Manager
func (e *Entry) Sync() error {
	return e.pm.file.Sync()
}

// Read tries to read len(p) bytes from the current cursor position
func (e *Entry) Read(p []byte) (n int, err error) {
	if len(e.pages) == 0 {
		return 0, io.EOF
	}

	// Get the amount of bytes the caller would like to read
	bytesToRead := int64(len(p))

	// Read until either length bytes were read or until we reached the end of
	// the last page
	copyDest := 0
	for bytesToRead > 0 {
		// Abort if no more pages are left to read
		if e.cursorPage >= int64(len(e.pages)) {
			break
		}

		readData := make([]byte, bytesToRead)

		// Read the data from the page
		var bytesRead int
		bytesRead, err = e.pages[e.cursorPage].readAt(readData, e.cursorOff)
		if err != nil {
			return 0, err
		}

		// Adjust the remaining bytesToRead and the cursor position
		bytesToRead -= int64(bytesRead)
		_, err = e.Seek(int64(bytesRead), io.SeekCurrent)
		if err != nil {
			return
		}

		// Copy data to output
		copy(p[copyDest:copyDest+bytesRead], readData)
		copyDest += bytesRead
	}

	// If no data was read signal the EOF
	if copyDest == 0 {
		return 0, io.EOF
	}

	return copyDest, nil
}

// Write tries to write len(p) byte to the current cursor position
func (e *Entry) Write(p []byte) (int, error) {
	// Get the amount of bytes the caller would like to write
	bytesToWrite := int64(len(p))

	// Inform the entryPage about new pages and the increase data usage
	byteIncrease := int64(0)
	addedPages := make([]*physicalPage, 0)

	// Write until all the bytes are written. If necessary allocate new pages
	writeCursor := 0
	for bytesToWrite > 0 {
		if e.cursorPage >= int64(len(e.pages)) {
			// Allocate new page if necessary
			newPage, err := e.pm.managedAllocatePage()
			if err != nil {
				return 0, err
			}
			// Add it to the list of pages and addedPages
			addedPages = append(addedPages, newPage)
			e.pages = append(e.pages, newPage)
			continue
		}

		// Write parts of the data to the page and remember the size increase
		// of the page
		page := e.pages[e.cursorPage]
		usedPageSize := page.usedSize
		bytesWritten, err := page.writeAt(p[writeCursor:], e.cursorOff)
		byteIncrease += (page.usedSize - usedPageSize)
		if err != nil {
			return 0, err
		}

		// Adjust the remaining bytesToWrite and the cursor position
		bytesToWrite -= int64(bytesWritten)
		_, err = e.Seek(int64(bytesWritten), io.SeekCurrent)
		if err != nil {
			return 0, err
		}

		// Increment the writeCursor of the input data
		writeCursor += bytesWritten
	}
	err := e.ep.addPages(addedPages, byteIncrease)
	if err != nil {
		return 0, build.ExtendErr("failed to add pages to entryPage", err)
	}

	return len(p), nil
}

// Seek moves the cursor for reading and writing to the appropriate page and
// offset
func (e *Entry) Seek(offset int64, whence int) (int64, error) {
	// Calculate the correct page and page offset
	var pageNum int64
	var pageOff int64

	switch whence {
	case io.SeekStart:
		pageNum = 0
		pageOff = 0
	case io.SeekCurrent:
		pageNum = e.cursorPage
		pageOff = e.cursorOff
	case io.SeekEnd:
		pageNum = int64(len(e.pages))
		pageOff = 0
	}

	// Don't allow to seek before start of file
	if pageNum*pageSize+pageOff+offset < 0 {
		return 0, errors.New("Cannot set cursor to negative position")
	}

	pageNumNew := (pageNum*pageSize + pageOff + offset) / pageSize
	pageOffNew := (pageNum*pageSize + pageOff + offset) % pageSize

	// If the page number is higher than the number of available pages set it to
	// the number of available pages at offset 0 to signal other functions that
	// we cannot continue reading
	if pageNumNew >= int64(len(e.pages)) {
		pageNumNew = int64(len(e.pages))
		pageOffNew = 0
	}

	e.cursorPage = pageNumNew
	e.cursorOff = pageOffNew
	return e.cursorPage*pageSize + e.cursorOff, nil
}

// ReadAt reads from a specific offset
func (e *Entry) ReadAt(p []byte, off int64) (n int, err error) {
	// Remember the cursor position
	tmpCursorPage := e.cursorPage
	tmpPageOff := e.cursorOff

	// Seek to the position from which we would like to read
	if _, err := e.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	// Read the data
	n, err = e.Read(p)

	// Restore the cursor position
	e.cursorPage = tmpCursorPage
	e.cursorOff = tmpPageOff
	return
}

// WriteAt writes to a specific offset
func (e *Entry) WriteAt(p []byte, off int64) (n int, err error) {
	// Remember the cursor position
	tmpCursorPage := e.cursorPage
	tmpPageOff := e.cursorOff

	if _, err := e.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	// Write data
	n, err = e.Write(p)

	// Restore the cursor position
	e.cursorPage = tmpCursorPage
	e.cursorOff = tmpPageOff
	return
}
