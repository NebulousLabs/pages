package pages

import (
	"bytes"
	"encoding/binary"
	"log"
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
		height uint64

		// parent points to the parent of the pageTable
		parent *pageTable

		// children are the pageTables the current pageTable is pointing to.
		childTables []*pageTable

		// childPages are the physical pages the currente pageTable is pointing to.
		childPages []*physicalPage

		// pp is the physical page on which the pageTable is stored
		pp *physicalPage

		// numPages is the number of pages stored in the tree of pageTables
		numPages uint64
	}
)

// newPageTable is a helper function to create a pageTable
func newPageTable(pm *PageManager) (*pageTable, error) {
	// Allocate a page for the table
	pp, err := pm.allocatePage()
	if err != nil {
		return nil, build.ExtendErr("failed to allocate page for new pageTable", err)
	}

	// Create and return the table
	pt := pageTable{
		pp: pp,
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
	newRoot.childTables = append(newRoot.childTables, root)
	newRoot.height = root.height + 1
	root.parent = newRoot

	// Complete the new branch of the tree
	err = fillPageTable(newRoot, pm)
	if err != nil {
		return nil, build.ExtendErr("Failed to fill new root", err)
	}
	return newRoot, nil
}

// fillPageTable is a helper function that fills the childTables field of a
// node recursively until it reaches the maximum size.
func fillPageTable(node *pageTable, pm *PageManager) error {
	if node.height == 0 {
		// This node directly points to pages. We are done.
		return nil
	}

	// Add page tables recursively until the array is full
	for len(node.childTables) < numPageEntries {
		// Create a new pageTable
		pt, err := newPageTable(pm)
		if err != nil {
			return build.ExtendErr("Failed to create new pageTable to extend the tree", err)
		}
		pt.height = node.height - 1
		pt.parent = node

		// Call fillPageTable on the new pageTable and append it
		err = fillPageTable(pt, pm)
		if err != nil {
			build.ExtendErr("Failed to fill pageTable", err)
		}
		node.childTables = append(node.childTables, pt)
	}
	return nil
}

// InsertPage inserts a page into the pageTable. The function returns the root
// of the pageTable tree since it might change due to an extension of the tree.
func (pt *pageTable) InsertPage(index uint64, pp *physicalPage, pm *PageManager) (*pageTable, error) {
	// If height equals 0 the current pt is a leaf and we can add the page directly
	if pt.height == 0 && len(pt.childPages) < numPageEntries {
		pt.childPages = append(pt.childPages, pp)
		root := pt
		// Find the new root
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
		return newRoot.InsertPage(index, pp, pm)
	}

	// Figure out which pageTable the page belongs to and call AddPage again
	// with the adjusted index
	tableIndex := index / numPageEntries
	newIndex := index % numPageEntries
	if index == numPageEntries {
		log.Printf("tableIndex %v, newIndex %v, entries %v",
			tableIndex, newIndex, len(pt.childTables))
		log.Printf("root height %v", pt.height)
	}
	return pt.childTables[tableIndex].InsertPage(newIndex, pp, pm)
}

// Marshal serializes a pageTable to be able to write it to disk
func (pt pageTable) Marshal() ([]byte, error) {
	buffer := bytes.NewBuffer(make([]byte, pt.Size()))

	// Get the type of the table, the number of entries and the offsets of the
	// entries
	var tableType uint32
	var numEntries uint32
	var offsets []int64
	if pt.height == 0 {
		tableType = pageDir
		numEntries = uint32(len(pt.childPages))
		for _, page := range pt.childPages {
			offsets = append(offsets, page.fileOff)
		}
	} else {
		tableType = tableDir
		numEntries = uint32(len(pt.childTables))
		for _, table := range pt.childTables {
			offsets = append(offsets, table.pp.fileOff)
		}
	}

	// Write table type
	err := binary.Write(buffer, binary.LittleEndian, &tableType)
	if err != nil {
		return nil, build.ExtendErr("Failed to marshal tableType", err)
	}

	// Write the number of entries
	err = binary.Write(buffer, binary.LittleEndian, &numEntries)
	if err != nil {
		return nil, build.ExtendErr("Failed to marshal numEntries", err)
	}

	// Write the offsets of the entries
	for _, offset := range offsets {
		err := binary.Write(buffer, binary.LittleEndian, &offset)
		if err != nil {
			return nil, build.ExtendErr("Failed to marshal entry offset", err)
		}
	}

	// Sanity check the marshalled length of the pageTable
	if buffer.Len() > pageSize {
		panic("Sanity check failed. Marshalled pagetable > pageSize")
	}

	return buffer.Bytes(), nil
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
