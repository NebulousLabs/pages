package pages

import (
	"bytes"
	"io"
	"testing"

	"github.com/NebulousLabs/fastrand"
)

// checkDataIntegrity is a helper function that reads len(data) bytes from a
// file starting at offste and checks if it is equal to data.
func checkDataIntegrity(pt *pagingTester, t *testing.T, offset int64, data []byte) {
	n := int64(len(data))
	readData := make([]byte, n)
	_, err := pt.pm.file.ReadAt(readData, offset)
	if err != nil {
		t.Error(err)
	}
	if bytes.Compare(data, readData) != 0 {
		t.Errorf("The data that was read didn't match the written data.")
	}
}

// TestPPWriteAt tests the functionality of the physicalPage's writeAt function
func TestPPWriteAt(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Create physicalPage
	physicalPage := physicalPage{
		file:     pt.pm.file,
		fileOff:  0,
		usedSize: 0,
	}

	// Write exactly pageSize bytes at the offset 0
	offset := int64(0)
	data := fastrand.Bytes(pageSize)
	n, err := physicalPage.writeAt(data, offset)
	if err != nil {
		t.Error(err)
	}
	if n != pageSize {
		t.Errorf("Should have written %v bytes but was %v", pageSize, n)
	}
	checkDataIntegrity(pt, t, offset, data[:int64(n)])

	// Write pageSize bytes to the middle of the page
	offset = 1000
	n, err = physicalPage.writeAt(data, offset)
	if err != nil {
		t.Error(err)
	}
	if int64(n) != pageSize-offset {
		t.Errorf("Should have written %v bytes but was %v", pageSize-offset, n)
	}
	checkDataIntegrity(pt, t, offset, data[:int64(n)])

	// Write pageSize bytes at an offset larger than pageSize-1
	offset = pageSize
	_, err = physicalPage.writeAt(data, offset)
	if err != io.EOF {
		t.Errorf("Error was %v but should have been %v", err, io.EOF)
	}

	// Write pageSize bytes at an offset smaller than 0
	offset = -1
	_, err = physicalPage.writeAt(data, offset)
	if err == nil {
		t.Error("This should have failed but didn't")
	}
}

// TestPPReadAt tests the functionality of the physicalPage's readAt function
func TestPPReadAt(t *testing.T) {
	pt, err := newPagingTester(t.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Create physicalPage
	physicalPage := physicalPage{
		file:     pt.pm.file,
		fileOff:  0,
		usedSize: 0,
	}

	// Write exactly pageSize bytes at the offset 0 to fill the whole page
	offset := int64(0)
	data := fastrand.Bytes(pageSize)
	n, err := physicalPage.writeAt(data, offset)
	if err != nil {
		t.Error(err)
	}
	if n != pageSize {
		t.Errorf("Should have written %v bytes but was %v", pageSize, n)
	}
	checkDataIntegrity(pt, t, offset, data[:int64(n)])

	// Read the page
	offset = 0
	dataRead := make([]byte, pageSize)
	n, err = physicalPage.readAt(dataRead, offset)
	if err != nil {
		t.Error(err)
	}
	if n != pageSize {
		t.Errorf("Should have read %v bytes but was %v", pageSize, n)
	}
	if bytes.Compare(data, dataRead) != 0 {
		t.Errorf("Read data doesn't match the written data")
	}

	// Read the page starting at a specific offset
	offset = 1000
	n, err = physicalPage.readAt(dataRead, offset)
	if err != nil {
		t.Error(err)
	}
	if int64(n) != pageSize-offset {
		t.Errorf("Should have read %v bytes but was %v", pageSize-offset, n)
	}
	if bytes.Compare(data[offset:], dataRead[:pageSize-offset]) != 0 {
		t.Errorf("Read data doesn't match the written data")
	}

	// Start reading at an offset >=pageSize
	offset = pageSize
	n, err = physicalPage.readAt(dataRead, offset)
	if err != io.EOF {
		t.Error(err)
	}

	// Start reading at a negative offset
	offset = -1
	n, err = physicalPage.readAt(dataRead, offset)
	if err == nil {
		t.Error("This should have failed but didn't")
	}
}
