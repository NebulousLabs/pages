package pages

import (
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
	freePages *recyclingPage

	// TODO find a better way to do this
	// recyclePages is used to indicate if it is safe to reuse free pages. This
	// is used as a workaround to disable page recycling while pages are being
	// inserted into freePages
	recyclePages bool

	// mu is a mutex to lock the PageManager's ressources
	mu *sync.Mutex

	// entryPages keeps track of all the entryPages
	entryPages map[Identifier]*entryPage
}

// allocatePage either returns a free page or allocates a page and adds
// it to the pages map.
func (p *PageManager) allocatePage() (*physicalPage, error) {
	// If there are free pages available return one of those
	var newPage *physicalPage
	if p.recyclePages && p.freePages != nil && p.freePages.len() > 0 {
		removedPage, err := p.freePages.freePage()
		if err != nil {
			return nil, build.ExtendErr("Failed to reuse free page", err)
		}
		removedPage.usedSize = 0
		return removedPage, nil

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

	// Don't start before dataOff
	if fileOff < dataOff {
		fileOff = dataOff
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

	// Allocate a page for the table
	pp, err := p.allocatePage()
	if err != nil {
		return nil, 0, build.ExtendErr("failed to allocate page for new entryPage", err)
	}

	// Create the first pageTable
	root, err := newPageTable(0, nil, p)
	if err != nil {
		return nil, 0, build.ExtendErr("Couldn't create new pageTable", err)
	}

	// Create the entryPage
	ep := &entryPage{
		&tieredPage{
			pp:   pp,
			pm:   p,
			root: root,
			mu:   new(sync.RWMutex),
		},
		0,
	}

	// Initialize entryPage
	if err := writeTieredPageEntry(pp, 0, 0, ep.pp.fileOff); err != nil {
		return nil, 0, err
	}

	// Create a new entry
	newEntry := &Entry{
		pm: p,
		ep: ep,
	}

	// Increment the entryPage's counter and add it to the map
	id := Identifier(ep.pp.fileOff)
	p.entryPages[id] = ep
	ep.instanceCounter++

	return newEntry, id, nil
}

// loadFreePagesFromDisk loads the offsets of free pages from the first page of
// the file.
func (p *PageManager) loadFreePagesFromDisk() error {
	// Create the physicalPage object using the identifier. We don't know
	// usedSize yet but for the entryPage we can just set it to pageSize
	pp := &physicalPage{
		file:     p.file,
		fileOff:  freeOff,
		usedSize: pageSize,
	}

	// Read all the entries from the entryPage and remember the root and usedSize
	rootOff := int64(0)
	usedSize := int64(0)
	height := int64(0)
	var err error
	for i := 0; i < pageSize/tieredPageEntrySize; i++ {
		usedSize, rootOff, err = readEntryPageEntry(pp, int64(i))
		if err != nil {
			return build.ExtendErr("Failed to read entry", err)
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
	ep := &recyclingPage{
		&tieredPage{
			pp:       pp,
			usedSize: usedSize,
			pm:       p,
			mu:       new(sync.RWMutex),
		},
	}

	// Recover the tree to get the pages of the entry
	if err := ep.recoverTree(rootOff, height); err != nil {
		return build.ExtendErr("Failed to recover tree", err)
	}

	p.freePages = ep
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
		mu:           new(sync.Mutex),
		entryPages:   make(map[Identifier]*entryPage),
		recyclePages: true,
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

	// Create the pageEntry for the free pages.
	root, err := newPageTable(0, nil, pm)
	if err != nil {
		return nil, build.ExtendErr("Failed to create pageTable for recycling page", err)
	}
	rp := &recyclingPage{
		&tieredPage{
			pm:   pm,
			root: root,
			mu:   new(sync.RWMutex),
			pp: &physicalPage{
				file:     pm.file,
				fileOff:  freeOff,
				usedSize: pageSize,
			},
		},
	}
	pm.freePages = rp

	return pm, nil
}

// Open loads a previously created entry
func (p *PageManager) Open(id Identifier) (*Entry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if the identifier was opened before
	if ep, exists := p.entryPages[id]; exists {
		// Increase the instance counter of the entryPage
		ep.instanceCounter++
		return &Entry{
			pm: p,
			ep: ep,
		}, nil
	}

	// Create the physicalPage object using the identifier. We don't know
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
	for i := 0; i < pageSize/tieredPageEntrySize; i++ {
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
		&tieredPage{
			pp:       pp,
			usedSize: usedSize,
			pm:       p,
			mu:       new(sync.RWMutex),
		},
		0,
	}

	// Recover the tree to get the pages of the entry
	if err := ep.recoverTree(rootOff, height); err != nil {
		return nil, build.ExtendErr("Failed to recover tree", err)
	}

	// Create the entry
	newEntry := &Entry{
		pm: p,
		ep: ep,
	}

	// Increment the entryPage's counter and add it to the map
	p.entryPages[id] = ep
	ep.instanceCounter++

	return newEntry, nil
}
