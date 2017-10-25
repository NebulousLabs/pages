package pages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/NebulousLabs/Sia/build"
)

type (
	// entryPage is the first page of an Entry. It points the pageTables of the
	// entry and stores the used data of the pageTables.
	entryPage struct {
		// root contains the root of the pageTable tree
		root *pageTable

		// pp is the physical page on which the entryPage is stored
		pp *physicalPage

		// usedSize is the size of the currently stored data in bytes
		usedSize int64

		// pm is the pageManager
		pm *PageManager
	}
)

// newEnryPage is a helper function to create an entryPage
func newEntryPage(pm *PageManager) (*entryPage, error) {
	// Allocate a page for the table
	pp, err := pm.allocatePage()
	if err != nil {
		return nil, build.ExtendErr("failed to allocate page for new entryPage", err)
	}

	// Create the first pageTable
	root, err := newPageTable(pm)
	if err != nil {
		return nil, build.ExtendErr("Couldn't create new pageTable", err)
	}

	// Create and return the entryPage
	ep := entryPage{
		pp:   pp,
		pm:   pm,
		root: root,
	}
	return &ep, nil
}

// AddPages adds multiple physical pages to the tree and increments the
// usedSize of the entryPage
func (ep *entryPage) addPages(pages []*physicalPage, addedBytes int64) error {
	// Add the pages to the entryPage
	index := uint64(ep.usedSize / pageSize)
	for _, page := range pages {
		newRoot, err := ep.root.insertPage(index, page, ep.pm)
		index++

		if err != nil {
			build.ExtendErr("failed to insert page", err)
		}

		// Check if root changed. If it did write down the entry for the last
		// root with it's max value for usedBytes before changing ep.root.
		if newRoot != ep.root {
			// numPages is the max number of pages the current root can contain
			numPages := int64(math.Pow(float64(numPageEntries), float64(ep.root.height+1)))

			// usedBytes is the max size of data the current root can contain
			usedBytes := numPages * pageSize

			writeEntryPageEntry(ep.pp, ep.root.height, usedBytes, ep.root.pp.fileOff)
			ep.root = newRoot
		}
	}

	// Increment the usedSize
	ep.usedSize += addedBytes

	// Write the root
	writeEntryPageEntry(ep.pp, ep.root.height, ep.usedSize, ep.root.pp.fileOff)

	return nil
}

// readEntryPageEntry reads the usedBytes of a pageTable and a ptr to the
// pageTable at a specific offset of a page from disk
func readEntryPageEntry(pp *physicalPage, index int64) (usedBytes int64, pageOff int64, err error) {
	// Read the data from disk and put it into a buffer
	entryData := make([]byte, entryPageEntrySize)
	_, err = pp.readAt(entryData, index*entryPageEntrySize)
	if err != nil {
		return
	}
	buffer := bytes.NewBuffer(entryData)

	// Read the usedBytes from the buffer
	if err = binary.Read(buffer, binary.LittleEndian, &usedBytes); err != nil {
		return
	}

	// Read the pageOff from the buffer
	err = binary.Read(buffer, binary.LittleEndian, &pageOff)
	return
}

// readPageTable read the tableType and entries of a pageTable
func readPageTable(pp *physicalPage) (entries []int64, err error) {
	pageData := make([]byte, pageSize)
	if _, err := pp.readAt(pageData, 0); err != nil {
		return nil, err
	}
	return unmarshalPageTable(pageData)
}

// recoverTree recovers the pageTable tree recursively starting at the offset
// of a pageTable
func (ep *entryPage) recoverTree(rootOff int64, height int64) (pages []*physicalPage, err error) {
	// Get the physicalPage for the rootOff
	pp := &physicalPage{
		file:     ep.pp.file,
		fileOff:  rootOff,
		usedSize: pageSize,
	}

	// Create the root object. Most of it's fields will be initialized in
	// recursiveRecovery
	root := &pageTable{
		pp:          pp,
		height:      height,
		childTables: make(map[uint64]*pageTable),
		childPages:  make(map[uint64]*physicalPage),
	}

	// Recover the tree recursively
	remainingBytes := ep.usedSize
	pages, err = recursiveRecovery(root, height, &remainingBytes)
	if err != nil {
		return
	}

	ep.root = root
	return
}

// recursiveRecovery is a helper function for recoverTree to recursively
// recover pageTables starting from a specific parent
func recursiveRecovery(parent *pageTable, height int64, remainingBytes *int64) (pages []*physicalPage, err error) {
	// Get the type and children of the table
	entries, err := readPageTable(parent.pp)
	if err != nil {
		return
	}

	// load children as pageTables
	for _, offset := range entries {
		pp := &physicalPage{
			file:     parent.pp.file,
			fileOff:  offset,
			usedSize: pageSize,
		}

		// Load children as pageTable
		if height > 0 {
			pt := &pageTable{
				height:      height,
				parent:      parent,
				childTables: make(map[uint64]*pageTable),
				childPages:  make(map[uint64]*physicalPage),
				pp:          pp,
			}

			p, err := recursiveRecovery(pt, height-1, remainingBytes)
			if err != nil {
				return nil, err
			}
			pages = append(pages, p...)

			// Set parent's fields
			parent.childTables[uint64(len(parent.childTables)-1)] = pt
			continue
		}

		// Load children as pages
		if height == 0 {
			if *remainingBytes > pageSize {
				pp.usedSize = pageSize
				*remainingBytes -= pageSize
			} else {
				pp.usedSize = *remainingBytes
				*remainingBytes = 0
			}
			// Set parent's fields
			parent.childPages[uint64(len(parent.childPages)-1)] = pp
			pages = append(pages, pp)
			continue
		}

		// Sanity check
		if height < 0 {
			panic("Sanity check failed. Height cannot be a negative value")
		}
	}

	return
}

// unmarshalPageTable a pageTable
func unmarshalPageTable(data []byte) (entries []int64, err error) {
	buffer := bytes.NewBuffer(data)

	// Unmarshal the number of entries in the table
	var numEntries uint64
	if err = binary.Read(buffer, binary.LittleEndian, &numEntries); err != nil {
		return
	}

	// Sanity check numEntries
	if numEntries > numPageEntries {
		panic(fmt.Sprintf("Sanity check failed. numEntries(%v) > numPageEntries(%v)",
			numEntries, numPageEntries))
	}

	// Unmarshal the entries
	var offset int64
	for i := uint64(0); i < numEntries; i++ {
		if err = binary.Read(buffer, binary.LittleEndian, &offset); err != nil {
			return
		}
		entries = append(entries, offset)
	}
	return
}

// writeEntryPageEntry writes the usedBytes of a pageTable and a ptr to the
// pageTable at a specific offset in the entryPage
func writeEntryPageEntry(pp *physicalPage, index int64, usedBytes int64, pageOff int64) error {
	buffer := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buffer, binary.LittleEndian, &usedBytes); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, &pageOff); err != nil {
		return err
	}
	if buffer.Len() != entryPageEntrySize {
		panic(fmt.Sprintf("pageEntry length %v != 16 bytes", buffer.Len()))
	}
	if _, err := pp.writeAt(buffer.Bytes(), index*entryPageEntrySize); err != nil {
		return err
	}
	return nil
}