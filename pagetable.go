package pages

import (
	"encoding/binary"
	"math"

	"github.com/NebulousLabs/Sia/build"
)

type (
	// pageTable is used to find pages associated with a certain group of
	// pages. It can either point to pages or to other pageTables not both.
	pageTable struct {
		// height indicates how far the pageTable is from the bottom layer of
		// the pageTable tree. A height of 0 indicates that the children of this
		// pageTable are pages
		height int64

		// parent points to the parent of the pageTable
		parent *pageTable

		// children are the pageTables the current pageTable is pointing to.
		childTables map[uint64]*pageTable

		// childPages are the physical pages the currente pageTable is pointing to.
		childPages map[uint64]*physicalPage

		// pp is the physical page on which the pageTable is stored
		pp *physicalPage
	}
)

// insertePage is a helper function that inserts a page into the pageTable
// tree. It returns the current root of the tree since it might change. This
// function is primarily intended to be used within addPage
func (pt *pageTable) insertPage(index uint64, pp *physicalPage, pm *PageManager) (*pageTable, error) {
	// If height equals 0 the current pt is a leaf and we can add the page directly
	if pt.height == 0 && len(pt.childPages) < numPageEntries {
		// Add the page and update the table on disk
		pt.childPages[index] = pp
		if err := pt.writeToDisk(); err != nil {
			return nil, err
		}

		// Find the new root
		root := pt
		for root.parent != nil {
			root = root.parent
		}
		return root, nil
	}

	// Check if we need to extend the tree
	maxPages := uint64(math.Pow(numPageEntries, float64(pt.height+1)))
	if index+1 > maxPages {
		newRoot, err := extendPageTableTree(pt, pm)
		if err != nil {
			return nil, build.ExtendErr("Failed to extend the pageTable tree", err)
		}
		return newRoot.insertPage(index, pp, pm)
	}

	// Figure out which pageTable the page belongs to and call AddPage again
	// with the adjusted index
	tableIndex := index / numPageEntries
	newIndex := index % numPageEntries

	// Check if the pageTable at tableIndex exists. If not, create it.
	if _, exists := pt.childTables[tableIndex]; !exists {
		newPt, err := newPageTable(pm)
		if err != nil {
			return nil, build.ExtendErr("Failed to create a new pageTable", err)
		}
		// Adjust the parent and the height. We don't need to write the changes
		// to disk right away. That will happen in the next recursive call to
		// insertPage
		newPt.parent = pt
		newPt.height = pt.height - 1

		// Update the child tables and save them to disk
		pt.childTables[tableIndex] = newPt
		if err := pt.writeToDisk(); err != nil {
			return nil, err
		}
	}
	return pt.childTables[tableIndex].insertPage(newIndex, pp, pm)
}

// newPageTable is a helper function to create a pageTable
func newPageTable(pm *PageManager) (*pageTable, error) {
	// Allocate a page for the table
	pp, err := pm.allocatePage()
	if err != nil {
		return nil, build.ExtendErr("failed to allocate page for new pageTable", err)
	}

	// Create and return the table
	pt := pageTable{
		pp:          pp,
		childPages:  make(map[uint64]*physicalPage),
		childTables: make(map[uint64]*pageTable),
	}
	return &pt, nil
}

// extendPageTableTree extends the pageTable tree by creating a new root,
// adding the current root as the first child and creating the rest of the tree
// structure
func extendPageTableTree(root *pageTable, pm *PageManager) (*pageTable, error) {
	if root.parent != nil {
		// This should only ever be called on the root node
		panic("Sanity check failed. Pt is not the root node")
	}

	// Create a new root pageTable
	newRoot, err := newPageTable(pm)
	if err != nil {
		return nil, build.ExtendErr("Failed to create new pageTable to extend the tree", err)
	}

	// Set the previous root pageTable to be the child of the new one
	newRoot.childTables[0] = root
	newRoot.height = root.height + 1
	root.parent = newRoot

	return newRoot, nil
}

// Marshal serializes a pageTable to be able to write it to disk
func (pt pageTable) Marshal() ([]byte, error) {
	// Get the number of entries and the offsets of the entries
	var numEntries uint64
	var offsets []int64
	if pt.height == 0 {
		numEntries = uint64(len(pt.childPages))
		for i := uint64(0); i < numEntries; i++ {
			offsets = append(offsets, pt.childPages[uint64(i)].fileOff)
		}
	} else {
		numEntries = uint64(len(pt.childTables))
		for i := uint64(0); i < numEntries; i++ {
			offsets = append(offsets, pt.childTables[uint64(i)].pp.fileOff)
		}
	}

	// off is an offset used for marshalling the data
	off := 0

	// Allocate enough memory for marshalled data
	data := make([]byte, (numEntries+1)*8)

	// Write the number of entries
	binary.LittleEndian.PutUint64(data[off:8], numEntries)
	off += 8

	// Write the offsets of the entries
	for _, offset := range offsets {
		binary.PutVarint(data[off:off+8], offset)
		off += 8
	}

	return data, nil
}

// writeToDisk marshals a pageTable and writes it to disk
func (pt pageTable) writeToDisk() error {
	// Marshal the pageTable
	data, err := pt.Marshal()
	if err != nil {
		return build.ExtendErr("Failed to marshal pageTable", err)
	}

	// Write it to disk
	_, err = pt.pp.writeAt(data, 0)
	if err != nil {
		return build.ExtendErr("Failed to write pageTable to disk", err)
	}

	return nil
}

// Size returns the length of the pageTable if it was marshalled
func (pt pageTable) Size() uint32 {
	// 4 Bytes for the tableType
	// 4 Bytes for the pageOffsts length
	// 8 * children bytes for the elements
	var children uint32
	if pt.height == 0 {
		children = uint32(len(pt.childPages))
	} else {
		children = uint32(len(pt.childTables))
	}
	return 4 + 4 + 8*children
}
