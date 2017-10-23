package pages

const (
	// pageSize is the size in bytes of a physical page on disk
	pageSize = 4096

	// numPageEntries is the number of entries that a marshalled pageTable can
	// point to. 4 bytes for the pageType, 4 bytes for the number of enteries and 8 for
	// each entry
	numPageEntries = (pageSize - 4 - 4) / 8.0

	// dataOff is the offset of the data relative to the start of the file.
	// Everything before that is metadata
	dataOff = 1 * pageSize
)

const (
	freePage = iota
	tableDir
	pageDir
)
