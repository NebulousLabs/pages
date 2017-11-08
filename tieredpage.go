package pages

// TODO whenever usedSize changes update the entry on disk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/NebulousLabs/Sia/build"
)

type (
	// tieredPage is a page with an underlying pageTable tree. It stores pages
	// and writes/loads them to/from disk
	tieredPage struct {
		// root contains the root of the pageTable tree
		root *pageTable

		// pp is the physical page on which the entryPage is stored
		pp *physicalPage

		// usedSize is the size of the currently stored data in bytes
		usedSize int64

		// pm is the pageManager
		pm *PageManager

		// pages is a list of all the physical pages of the tree
		pages []*physicalPage

		// mu is used to lock all operations on the entries
		mu *sync.RWMutex
	}

	// entryPage is the first page of an Entry.
	entryPage struct {
		// entryPage is a tieredPage
		*tieredPage

		// atomicInstanceCounter counts the number of open references to the
		// entryPage. It is increased in Open and decreased in Close
		instanceCounter uint64
	}

	// recyclingPage is a tiered page that stores all the free pages
	recyclingPage struct {
		// recyclingPage is a tieredPage
		*tieredPage
	}
)

// AddPages adds multiple physical pages to the tree and increments the
// usedSize of the entryPage. The ep.mu write lock needs to be acquired if
// len(pages) > 0 otherwise the read lock will suffice
func (ep *entryPage) addPages(pages []*physicalPage, addedBytes int64) error {
	if addedBytes == 0 {
		return nil
	}

	// Otherwise add the pages to the entryPage
	index := ep.len()
	for _, page := range pages {
		root := ep.root
		if err := ep.insertPage(index, page); err != nil {
			return build.ExtendErr("failed to insert page", err)
		}

		// Check if root changed. If it did write down the entry for the last
		// root with it's max value for usedBytes before changing ep.root.
		if root != ep.root {
			bytesUsed := int64(maxPages(root.height) * pageSize)
			if err := writeTieredPageEntry(ep.pp, root.height, bytesUsed, root.pp.fileOff); err != nil {
				return err
			}
		}
		index++
	}

	// Increment the usedSize
	ep.usedSize += addedBytes

	// Write the root
	return writeTieredPageEntry(ep.pp, ep.root.height, ep.usedSize, ep.root.pp.fileOff)
}

// AddPages adds multiple physical pages to the tree and increments the
// usedSize of the entryPage. The ep.mu write lock needs to be acquired if
// len(pages) > 0 otherwise the read lock will suffice
func (rp *recyclingPage) addPages(pages []*physicalPage) error {
	// Stop recycling while pages are added
	rp.pm.recyclePages = false
	defer func() {
		rp.pm.recyclePages = true
	}()

	// Otherwise add the pages to the entryPage
	index := rp.len()
	for _, page := range pages {
		// free pages are treated as if they were full
		page.usedSize = pageSize

		root := rp.root
		if err := rp.insertPage(index, page); err != nil {
			return build.ExtendErr("failed to insert page", err)
		}

		// Check if root changed. If it did write down the entry for the last
		// root with it's max value for usedBytes before changing ep.root.
		if root != rp.root {
			bytesUsed := int64(maxPages(root.height) * pageSize)
			if err := writeTieredPageEntry(rp.pp, root.height, bytesUsed, root.pp.fileOff); err != nil {
				return err
			}
		}
		index++
	}

	// Add pages to rp.pages
	rp.pages = append(rp.pages, pages...)

	// Increment the usedSize
	rp.usedSize += int64(len(pages)) * pageSize

	// Write the root
	return writeTieredPageEntry(rp.pp, rp.root.height, rp.usedSize, rp.root.pp.fileOff)
}

// defrag needs to be called after entry operation that possibly removes
// pageTables from the tree. Itwrites the current usedSize to disk and reduces
// the height of the tree if possible.
func (tp *tieredPage) defrag() error {
	// Write current usedSize to disk
	if err := writeTieredPageEntry(tp.pp, tp.root.height, tp.usedSize, tp.root.pp.fileOff); err != nil {
		return err
	}

	// Defrag until the root node has multiple children
	var err error
	var pagesToFree []*physicalPage
	for tp.root.height > 0 && len(tp.root.childTables) == 1 {
		child := tp.root.childTables[0]

		// Write the previous pageEntry's entry
		err = writeTieredPageEntry(tp.pp, child.height, tp.usedSize, child.pp.fileOff)
		if err != nil {
			return err
		}

		// Zero out the current entry
		err = writeTieredPageEntry(tp.pp, tp.root.height, 0, 0)
		if err != nil {
			return err
		}

		// remember to free current root page. We can't do it right away since
		// there is a chance that the tieredPage's root changes when we call
		// addPages
		pagesToFree = append(pagesToFree, tp.root.pp)

		// change root to its child
		tp.root = tp.root.childTables[0]
	}

	// Free pages
	// TODO this might cause pages to get lost if a power outage occurs after
	// modifying the tieredPage but before freeing the pages
	err = tp.pm.freePages.addPages(pagesToFree)
	if err != nil {
		return err
	}
	return nil
}

// len returns the number of pages currently stored in the tree
func (tp *tieredPage) len() uint64 {
	return uint64(tp.usedSize / pageSize)
}

// maxPages return the number of pages the tree can contain
func (tp *tieredPage) maxPages() uint64 {
	return maxPages(tp.root.height)
}

// cap returns the number of pages a tree with a certain height can contain.
// The height starts at 0. This means a simple tree with 1 root node and
// numPageEntries leaves would have height 1
func maxPages(height int64) uint64 {
	return uint64(math.Pow(numPageEntries, float64(height+1)))
}

// insertePage is a helper function that inserts a page into the pageTable
// tree. It returns an error to indicate if the root changed.
func (tp *tieredPage) insertPage(index uint64, pp *physicalPage) error {
	// Calculate the maximum number of pages the tree can contain at the moment
	// If the index is too large we need to extend the tree before we can
	// insert the page
	for maxPages := tp.maxPages(); index >= maxPages; maxPages = tp.maxPages() {
		newRoot, err := extendPageTableTree(tp.root, tp.pm)
		if err != nil {
			return build.ExtendErr("Failed to extend the pageTable tree", err)
		}
		tp.root = newRoot
	}

	// Search the tree for the correct pageTable to insert the page
	pt := tp.root
	var tableIndex uint64
	var pageIndex = index
	for pt.height > 0 {
		tableIndex = pageIndex / maxPages(pt.height-1)
		pageIndex /= numPageEntries

		// Check if the pageTable exists. If it doesn't, we have to create it
		_, exists := pt.childTables[tableIndex]
		if !exists {
			newPt, err := newPageTable(pt.height-1, pt, tp.pm)
			if err != nil {
				return build.ExtendErr("failed to create a new pageTable", err)
			}
			pt.childTables[tableIndex] = newPt
			if err := pt.writeToDisk(); err != nil {
				return build.ExtendErr("failed to write pageTable to disk", err)
			}
		}
		pt = pt.childTables[tableIndex]
	}

	// Sanity check the child pages
	if len(pt.childPages) == numPageEntries {
		panic(fmt.Sprintf("We shouldn't insert if childPages is already full: index %v", index))
	}
	if len(pt.childPages) > 0 && pt.childPages[index%numPageEntries-1] == nil {
		panic("Inserting shouldn't create a gap")
	}

	// Insert page
	pt.childPages[index%numPageEntries] = pp
	if err := pt.writeToDisk(); err != nil {
		return err
	}
	return nil
}

// page returns a page at a given index from the tree
// TODO Maybe delete this
func (tp *tieredPage) page(index uint64) (*physicalPage, error) {
	pt := tp.root
	var tableIndex uint64
	var pageIndex = index
	var exists bool

	// Loop until page is found
	for pt.height > 0 {
		tableIndex = pageIndex / maxPages(pt.height-1)
		pageIndex /= numPageEntries
		pt, exists = pt.childTables[tableIndex]
		if !exists {
			return nil, fmt.Errorf("table at index %v doesn't exist", tableIndex)
		}
	}

	// Get the page
	page, exists := pt.childPages[index%numPageEntries]
	if !exists {
		return nil, fmt.Errorf("page at index %v doesn't exist", pageIndex)
	}
	return page, nil
}

// removePage removes a page at a given index from the tree and returns the
// deleted page
// TODO this function is similar to truncate. Maybe there is a way to call
// recursiveTruncate here. Currently this is not possible since
// recursiveTruncate instantly adds pages to the free pages tree which results
// in weird behavior.
func (rp *recyclingPage) freePage() (*physicalPage, error) {
	pt := rp.root
	var index = rp.len() - 1
	var tableIndex uint64
	var pageIndex = index
	var exists bool

	// Stop recycling while pages are added
	rp.pm.recyclePages = false
	defer func() {
		rp.pm.recyclePages = true
	}()

	// Loop until page is found
	for pt.height > 0 {
		tableIndex = pageIndex / maxPages(pt.height-1)
		pageIndex /= numPageEntries
		pt, exists = pt.childTables[tableIndex]
		if !exists {
			return nil, fmt.Errorf("table at index %v doesn't exist", tableIndex)
		}
	}

	// Sanity check if deleting the page is safe
	if _, exists := pt.childPages[index%numPageEntries+1]; exists {
		panic("deleting page would create a gap")
	}

	// Delete the page
	page, exists := pt.childPages[index%numPageEntries]
	if !exists {
		return nil, fmt.Errorf("page at index %v doesn't exist", pageIndex)
	}

	delete(pt.childPages, index%numPageEntries)
	rp.usedSize -= page.usedSize

	// Write modified pageTable to disk
	if err := pt.writeToDisk(); err != nil {
		return nil, err
	}

	// If there are no more pages left and pt is not the root we can delete the
	// pageTable
	var pagesToFree []*physicalPage
	if pt.parent != nil && len(pt.childPages) == 0 {
		// Add its page to the free ones
		pagesToFree = append(pagesToFree, pt.pp)

		// Update the parent on disk
		delete(pt.parent.childTables, uint64(len(pt.parent.childTables)-1))
		if err := pt.parent.writeToDisk(); err != nil {
			return nil, err
		}
	}

	// Go through the parents and delete empty ones
	for pt = pt.parent; pt.parent != nil && len(pt.childTables) == 0; pt = pt.parent {
		// Add its page to the free ones
		pagesToFree = append(pagesToFree, pt.pp)

		// Update the parent on disk
		delete(pt.parent.childTables, uint64(len(pt.parent.childTables)-1))
		if err := pt.parent.writeToDisk(); err != nil {
			return nil, err
		}
	}

	// Sanity check if last page of rp.pages equals the removed page
	if page != rp.pages[len(rp.pages)-1] {
		panic("removed page isn't the same as the last page in rp.pages")
	}

	// Remove the last page from rp.pages
	rp.pages = rp.pages[:len(rp.pages)-1]

	return page, nil
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
func (tp *tieredPage) recoverTree(rootOff int64, height int64) (err error) {
	// Get the physicalPage for the rootOff
	pp := &physicalPage{
		file:     tp.pp.file,
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
	remainingBytes := tp.usedSize
	tp.pages, err = recursiveRecovery(root, height, &remainingBytes)
	if err != nil {
		return
	}

	tp.root = root
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

// recursiveTruncate is a helper function that recursively walks over the
// allocated pages and deletes them until a certain size is reached
func (tp *tieredPage) recursiveTruncate(pt *pageTable, size int64) (bool, error) {
	// Call recursiveTruncate on child tables
	if pt.height > 0 {
		for i := uint64(len(pt.childTables)) - 1; i >= 0; i-- {
			// Stop if entry is small enough
			if tp.usedSize <= size {
				return false, nil
			}

			// Otherwise call truncate recursively
			empty, err := tp.recursiveTruncate(pt.childTables[i], size)
			if err != nil {
				return false, err
			}

			// If the child is empty now we can remove it from the tree and
			// free its page
			if empty {
				// Delete and clear the child
				child := pt.childTables[i]
				delete(pt.childTables, i)

				// Add its page to the free ones
				err := tp.pm.freePages.addPages([]*physicalPage{child.pp})
				if err != nil {
					return false, err
				}

				// Update pt on disk
				if err := pt.writeToDisk(); err != nil {
					return false, err
				}

				// If the parent is now empty too return
				if len(pt.childTables) == 0 {
					return true, nil
				}
			}
		}
	}

	// Start removing pages
	if pt.height == 0 {
		for i := uint64(len(pt.childPages)) - 1; i >= 0; i-- {
			// Stop if entry is small enough
			if tp.usedSize <= size {
				return false, nil
			}
			page := pt.childPages[i]

			// Check if we need to remove the whole page or if we can just
			// truncate it
			remainingTruncation := tp.usedSize - size
			if remainingTruncation < page.usedSize {
				page.usedSize = page.usedSize - remainingTruncation
				tp.usedSize -= remainingTruncation
				continue
			}

			// Remove the page from the entry's pages and the pageTable
			delete(pt.childPages, i)
			removed := tp.pages[len(tp.pages)-1]
			tp.pages = tp.pages[:len(tp.pages)-1]

			// Sanity check. Removed pages should be the same
			if removed.fileOff != page.fileOff {
				panic(fmt.Sprintf("removed pages weren't the same %v != %v",
					removed.fileOff, page.fileOff))
			}

			// add the page to the pageManager's freePages
			err := tp.pm.freePages.addPages([]*physicalPage{page})
			if err != nil {
				return false, nil
			}

			// Clear the removed page
			tp.usedSize -= page.usedSize

			// If the childTables are empty we can return right away
			if len(pt.childPages) == 0 {
				return true, nil
			}
		}
		return false, nil
	}

	// sanity check height
	panic("sanity check failed. height can't be a negative value.")
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
	numEntries := binary.LittleEndian.Uint64(data[off:8])
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

// writeTieredPageEntry writes the usedBytes of a pageTable and a ptr to the
// pageTable at a specific offset in the entryPage
func writeTieredPageEntry(pp *physicalPage, index int64, usedBytes int64, pageOff int64) error {
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
