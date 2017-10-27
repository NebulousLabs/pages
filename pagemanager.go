package pages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/NebulousLabs/Sia/build"
)

// Identifier is a helper type that can be used to reopen a previously created
// entry
type Identifier int64

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

	// Get the fileOff for the page
	fileOff, err := p.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// The last page might not have pageSize yet so we might have to adjust the
	// offset a bit
	if fileOff%pageSize != 0 {
		fileOff += (pageSize - fileOff%pageSize)
	}

	// Create the new page and write it to disk
	newPage = &physicalPage{
		file:    p.file,
		fileOff: fileOff,
	}

	// TODO maybe remove this but if we do we have to fix the way we calculate
	// the fileOff for new pages
	n, err := newPage.file.WriteAt(make([]byte, pageSize, pageSize), newPage.fileOff)
	if n != pageSize || err != nil {
		return nil, fmt.Errorf("couldn't write new page wrote %v bytes %v", n, err)
	}

	// Increment the number of allocated pages
	p.allocatedPages++

	return newPage, nil
}

// Close closes open handles and frees ressources
func (p PageManager) Close() error {
	return p.file.Close()
}

// Create creates a new Entry and returns an identifier for it
func (p *PageManager) Create() (*Entry, Identifier, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create entryPage
	ep, err := newEntryPage(p)
	if err != nil {
		build.ExtendErr("Failed to create entryPage", err)
	}

	// Create a new entry
	newEntry := &Entry{
		pm: p,
		ep: ep,
	}

	return newEntry, Identifier(ep.pp.fileOff), nil
}

// loadFreePagesFromDisk loads the offsets of free pages from the first page of
// the file.
func (p *PageManager) loadFreePagesFromDisk() error {
	// Read the whole page. We need to check for EOF in case the filesize is
	// smaller than pageSize which might happen if the PageManager is created
	// and closed before writing any files to it other than the initial free
	// pages
	pageData := make([]byte, pageSize)
	if n, err := p.file.ReadAt(pageData, freePagesOffset); err != nil && !(err == io.EOF && n > 0) {
		return err
	}

	// Create buffer from the pages data and read the number of entries
	buffer := bytes.NewBuffer(pageData)
	numEntries := uint64(0)
	if err := binary.Read(buffer, binary.LittleEndian, &numEntries); err != nil {
		return err
	}

	var offset int64
	for i := uint64(0); i < numEntries; i++ {
		// Unmarshal page offset
		if err := binary.Read(buffer, binary.LittleEndian, &offset); err != nil {
			return err
		}

		// Create physicalPage object
		pp := &physicalPage{
			file:    p.file,
			fileOff: offset,
		}

		// Append it to the pageManager
		p.freePages = append(p.freePages, pp)
	}
	return nil
}

// managedAllocatePage either returns a free page or allocates a page and adds
// it to the pages map.
func (p *PageManager) managedAllocatePage() (*physicalPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.allocatePage()
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

		// Load the freePages
		if err := pm.loadFreePagesFromDisk(); err != nil {
			return nil, build.ExtendErr("failed to read free pages", err)
		}
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

	// Init the free pages metadata
	if err := pm.writeFreePagesToDisk(); err != nil {
		return nil, build.ExtendErr("Failed to write free page to disk", err)
	}

	return pm, nil
}

// Open loads a previously created entry
func (p *PageManager) Open(id Identifier) (*Entry, error) {
	// Create the physicalPage object the identifier. We don't know
	// usedSize yet but for the entryPage we can just set it to pageSize
	pp := &physicalPage{
		file:     p.file,
		fileOff:  int64(id),
		usedSize: pageSize,
	}

	// Read all the entries from the entryPage and remember the root and usedSize
	rootOff := int64(0)
	usedSize := int64(0)
	height := int64(0)
	var err error
	for i := 0; i < pageSize/entryPageEntrySize; i++ {
		usedSize, rootOff, err = readEntryPageEntry(pp, int64(i))
		if err != nil {
			return nil, build.ExtendErr("Failed to read entry", err)
		}

		// Remember the reached height
		height = int64(i)

		// Stop if we find a root that isn't full yet
		numPages := int64(math.Pow(float64(numPageEntries), float64(i+1)))
		if usedSize < numPages*pageSize {
			break
		}
	}

	// Create the entryPage object and recover the tree.
	ep := &entryPage{
		pp:       pp,
		usedSize: usedSize,
		pm:       p,
	}

	// Recover the tree to get the pages of the entry
	pages, err := ep.recoverTree(rootOff, height)
	if err != nil {
		return nil, build.ExtendErr("Failed to recover tree", err)
	}

	// Create the entry
	newEntry := &Entry{
		pm:    p,
		ep:    ep,
		pages: pages,
	}

	return newEntry, nil
}

// writeFreePagesToDisk writes the offsets of the freePages of the pageManager
// to disk on the first page of the file
func (p *PageManager) writeFreePagesToDisk() error {
	buffer := bytes.NewBuffer(make([]byte, 0))

	// Get the number of pages we are about to write to disk
	numPages := uint64(len(p.freePages))
	if numPages > maxFreePagesStored {
		numPages = maxFreePagesStored
	}

	// Write the number of pages to the buffer
	if err := binary.Write(buffer, binary.LittleEndian, &numPages); err != nil {
		return err
	}

	// Write each page to the buffer
	for i := uint64(0); i < numPages; i++ {
		if err := binary.Write(buffer, binary.LittleEndian, &p.freePages[i].fileOff); err != nil {
			return err
		}
	}

	// Sanity check buffer length
	if buffer.Len() > pageSize {
		panic("Sanity check failed. Buffer length larger than filesize")
	}

	// Write buffer to disk
	if _, err := p.file.WriteAt(buffer.Bytes(), freePagesOffset); err != nil {
		return err
	}
	return nil
}
