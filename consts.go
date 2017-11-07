package pages

const (
	// pageSize is the size in bytes of a physical page on disk
	pageSize = 4096

	// entryPageEntrySize is the size of an entry in the entryPage
	entryPageEntrySize = 16

	// numPageEntries is the number of entries that a marshalled pageTable can
	// point to. 8 bytes for the number of entries and 8 for each entry
	numPageEntries = (pageSize - 8) / 8.0

	// freeOff is the offset of the freePages entryPage relative to the start
	// of the file
	freeOff = 0

	// dataOff is the offset of the data relative to the start of the file.
	dataOff = 1 * pageSize

	// freePagesOffset is the offset at which the free pages of the pageManager
	// are stored on disk relative to the start of the file. Only
	// maxFreePagesStored pages can be stored there but the defrag thread is
	// constantly looking for new pages
	freePagesOffset = 0
)
