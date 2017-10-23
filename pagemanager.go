package pages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/NebulousLabs/Sia/build"
)

// PageManager blabla
type PageManager struct {
	// file is the underlying file to which data is written
	file *os.File

	// freePages contains the pages that can be reused for new data
	freePages []*physicalPage

	// mu is a mutex to lock the PageManager's ressources
	mu *sync.Mutex

	// allocatedPages keeps track of the number of allocated pages
	allocatedPages int64
}

// allocatePage either returns a free page or allocates a page and adds
// it to the pages map.
func (p *PageManager) allocatePage() (*physicalPage, error) {
	// If there are free pages available return one of those
	var newPage *physicalPage
	if len(p.freePages) > 0 {
		newPage = p.freePages[0]
		p.freePages = p.freePages[1:]
		return newPage, nil
	}

	// Create the new page and write it to disk
	newPage = &physicalPage{
		file:    p.file,
		fileOff: p.allocatedPages*pageSize + dataOff,
	}

	// TODO maybe remove this
	if n, err := newPage.writeAt(make([]byte, pageSize, pageSize), 0); n != pageSize || err != nil {
		return nil, fmt.Errorf("couldn't write new page wrote %v bytes %v", n, err)
	}

	// Increment the number of allocated pages
	p.allocatedPages++

	return newPage, nil
}

// managedAllocatePage either returns a free page or allocates a page and adds
// it to the pages map.
func (p *PageManager) managedAllocatePage() (*physicalPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocatePage()
}

// Open opens an existing Entry and loads it's pageTables into memory
func (p PageManager) Open(identifier uint64) (*Entry, error) {
	panic("not implemented yet")
}

func recursivelyLoadPageTables(identifier uint64) (uint32, error) {
	panic("not implemented yet")
}

// loadPageTable loads a single pageTable from disk and returns it's type and
// the offsets it is pointing to
func (p *PageManager) loadPageTable(offset int64) (pageType uint32, entries []uint64, err error) {
	// Sanity check offset
	if offset%pageSize != 0 {
		panic("sanity check failed, offset is not at start of a page")
	}

	// Read the page
	pageData := make([]byte, pageSize)
	_, err = p.file.ReadAt(pageData, offset)
	if err != nil {
		return
	}

	// Unmarshal pageType
	buffer := bytes.NewBuffer(pageData)
	err = binary.Read(buffer, binary.LittleEndian, &pageType)
	if err != nil {
		return
	}

	// Unmarshal the entriesLength
	var entriesLength uint32
	err = binary.Read(buffer, binary.LittleEndian, &entriesLength)
	if err != nil {
		return
	}

	// Unmarshal entries
	var entryOff uint64
	for i := uint32(0); i < entriesLength; i++ {
		err = binary.Read(buffer, binary.LittleEndian, &entryOff)
		if err != nil {
			return
		}
		entries = append(entries, entryOff)
	}
	return
}

// Create creates a new Entry and returns an identifier for it
func (p *PageManager) Create() (*Entry, int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create pageTable
	pt, err := newPageTable(p)
	if err != nil {
		build.ExtendErr("Failed to create pageTable", err)
	}

	// Create a new entry
	newEntry := &Entry{
		pm: p,
		pt: pt,
	}

	return newEntry, pt.pp.fileOff, nil
}

// Close closes open handles and frees ressources
func (p PageManager) Close() error {
	return p.file.Close()
}

// Recover loads the metadata from disk
func (p *PageManager) recover() error {
	panic("not implemented")
}

// New creates a PageManager or recovers an existing one
func New(filePath string) (*PageManager, error) {
	// Create the page manager object
	pm := &PageManager{
		mu: new(sync.Mutex),
	}

	// Try to open the database file
	file, err := os.OpenFile(filePath, os.O_RDWR, 0600)
	if err == nil {
		// There is a file that can be recovered
		pm.file = file
		return pm, pm.recover()
	} else if !os.IsNotExist(err) {
		// The file exists but cannot be opened
		return nil, build.ExtendErr("Failed to open existing database file", err)
	}

	// If the file doesn't exist create a new one
	file, err = os.Create(filePath)
	if err != nil {
		return nil, build.ExtendErr("Failed to create the database file: %v", err)
	}
	pm.file = file

	// TODO Initialize metadata

	return pm, nil
}
