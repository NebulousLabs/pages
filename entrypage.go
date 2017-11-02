package pages

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"

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

		// atomicInstanceCounter counts the number of open references to the
		// entryPage. It is increased in Open and decreased in Close
		instanceCounter uint64

		// pages is a list of all the physical pages of the tree
		pages []*physicalPage

		// mu is used to lock all operations on the entries
		mu *sync.RWMutex
	}
)

// AddPages adds multiple physical pages to the tree and increments the
// usedSize of the entryPage. The ep.mu write lock needs to be acquired if
// len(pages) > 0 otherwise the read lock will suffice
func (ep *entryPage) addPages(pages []*physicalPage, addedBytes int64) error {
	if addedBytes == 0 {
		return nil
	}

	log.Printf("addpages start")
	defer log.Printf("addpages end")
	// Otherwise add the pages to the entryPage
	index := uint64(ep.usedSize / pageSize)
	for _, page := range pages {
		log.Printf("index %v", index)
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
	// Read the data from disk
	entryData := make([]byte, entryPageEntrySize)
	_, err = pp.readAt(entryData, index*entryPageEntrySize)
	if err != nil {
		return
	}

	// Unmarshal the usedBytes
	var bytesRead int
	if usedBytes, bytesRead = binary.Varint(entryData[0:8]); usedBytes == 0 && bytesRead <= 0 {
		err = errors.New("Failed to unmarshal usedBytes")
		return
	}

	// Unmarshal the pageOff
	if pageOff, bytesRead = binary.Varint(entryData[8:]); pageOff == 0 && bytesRead <= 0 {
		err = errors.New("Failed to unmarshal entryData")
		return
	}
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
func (ep *entryPage) recoverTree(rootOff int64, height int64) (err error) {
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
	ep.pages, err = recursiveRecovery(root, height, &remainingBytes)
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
	// The data should be at least 8 bytes long
	if len(data) < 8 {
		panic("input data is too shot")
	}

	// off is a offset used for unmarshaling the data
	off := 0

	// Unmarshal the number of entries in the table
	numEntries := binary.LittleEndian.Uint64(data[0:8])
	off += 8

	// Sanity check numEntries
	if numEntries > numPageEntries {
		panic(fmt.Sprintf("Sanity check failed. numEntries(%v) > numPageEntries(%v)",
			numEntries, numPageEntries))
	}

	// Sanity check the remaining data length
	if uint64(len(data[off:])) < numEntries*8 {
		panic(fmt.Sprintf("Sanity check failed. %v < %v", len(data[off:]), numEntries*8))
	}

	// Unmarshal the entries
	for i := uint64(0); i < numEntries; i++ {
		offset, bytesRead := binary.Varint(data[off : off+8])
		if offset == 0 && bytesRead <= 0 {
			err = errors.New("Failed to unmarshal offset")
			return
		}
		off += 8
		entries = append(entries, offset)
	}
	return
}

// writeEntryPageEntry writes the usedBytes of a pageTable and a ptr to the
// pageTable at a specific offset in the entryPage
func writeEntryPageEntry(pp *physicalPage, index int64, usedBytes int64, pageOff int64) error {
	data := make([]byte, entryPageEntrySize)

	// Marshal usedBytes and pageOff
	binary.PutVarint(data[0:8], usedBytes)
	binary.PutVarint(data[8:], pageOff)

	// Write the data to disk
	if _, err := pp.writeAt(data, index*entryPageEntrySize); err != nil {
		return err
	}
	return nil
}
