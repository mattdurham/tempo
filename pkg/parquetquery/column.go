package parquetquery

import (
	"sync"

	"github.com/parquet-go/parquet-go"
)

type ColumnChunkHelper struct {
	parquet.ColumnChunk
	pages     parquet.Pages
	firstPage parquet.Page
	err       error
}

var columnChunkHelperPool = sync.Pool{
	New: func() interface{} {
		return &ColumnChunkHelper{}
	},
}

func getColumnChunkHelper(cc parquet.ColumnChunk) *ColumnChunkHelper {
	h := columnChunkHelperPool.Get().(*ColumnChunkHelper)
	h.ColumnChunk = cc
	h.err = nil
	return h
}

func putColumnChunkHelper(h *ColumnChunkHelper) {
	// Clear the interface field so GC can release the underlying column chunk.
	h.ColumnChunk = nil
	columnChunkHelperPool.Put(h)
}

// Dictionary makes it easier to access the dictionary for this column chunk which
// is only accessible through the first page. Internally keeps some open buffers
// to reuse later which are accessed through the other methods. If there is no dictionary
// for this column chunk or an error occurs, return nil.
func (h *ColumnChunkHelper) Dictionary() parquet.Dictionary {
	if h.pages == nil {
		h.pages = h.Pages()
	}

	if h.firstPage == nil {
		h.firstPage, h.err = h.pages.ReadPage()
	}

	if h.firstPage == nil {
		// Maybe there was an error
		return nil
	}

	return h.firstPage.Dictionary()
}

// NextPage wraps pages.ReadPage and helps reuse already open buffers.
func (h *ColumnChunkHelper) NextPage() (parquet.Page, error) {
	if h.err != nil {
		return nil, h.err
	}

	if h.firstPage != nil {
		// Clear and return the already buffered first page.
		// Caller takes ownership of it.
		pg := h.firstPage
		h.firstPage = nil
		return pg, nil
	}

	if h.pages == nil {
		h.pages = h.Pages()
	}

	return h.pages.ReadPage()
}

func (h *ColumnChunkHelper) Close() error {
	if h.firstPage != nil {
		parquet.Release(h.firstPage)
		h.firstPage = nil
	}

	if h.pages != nil {
		err := h.pages.Close()
		h.pages = nil
		return err
	}

	return nil
}
