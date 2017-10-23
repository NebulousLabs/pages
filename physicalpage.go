package pages

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type (
	// physicalPage is a helper struct to easily write/read pages to/from disk
	physicalPage struct {
		// file is the file on which the page is stored
		file *os.File

		// fileOff is the offset of the page to the beginning of the file
		fileOff int64

		// usedSize is the amount of bytes of the page that are currently in
		// use
		usedSize int64
	}
)

// readAt reads the contents of a physical page starting from a specific
// offset.
func (p *physicalPage) readAt(b []byte, off int64) (n int, err error) {
	// Check if the offset is in range
	if off >= p.usedSize {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, errors.New("Cannot read at negative offset")
	}

	// Define the range to read
	length := int64(len(b))
	if length > p.usedSize-off {
		length = p.usedSize - off
	}

	data := make([]byte, length)
	n, err = p.file.ReadAt(data, p.fileOff+off)
	if int64(n) != length {
		panic(fmt.Sprintf("Sanity Check: ReadAt should have read %v bytes", length))
	}

	copy(b, data)
	return
}

// writeAt writes data to a physical page starting from a specific offset.
func (p *physicalPage) writeAt(b []byte, off int64) (n int, err error) {
	// Check if the offset is in range
	if off >= pageSize {
		return 0, io.EOF
	}
	if off < 0 {
		return 0, errors.New("Cannot write at negative offset")
	}

	// Calculate how much we can write to the page
	length := int64(len(b))
	if length > pageSize-off {
		length = pageSize - off
	}

	n, err = p.file.WriteAt(b[:length], p.fileOff+off)

	// Update the usedSize if necessary
	if off+length > p.usedSize {
		p.usedSize = off + length
	}

	if int64(n) != length {
		panic(fmt.Sprintf("Sanity Check: WriteAt should have written %v bytes", length))
	}
	return
}
