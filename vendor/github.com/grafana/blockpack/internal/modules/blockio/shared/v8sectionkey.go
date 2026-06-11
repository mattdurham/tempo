package shared

// V8SectionKey identifies one V8 per-column/per-index blob by its routing
// (TocType, SubType) and Name. It lets a caller batch keys that span more than
// one TocType (e.g. a block's ToC plus its columns) into a single cache GetMulti
// (NOTE-185), provided they all route to the same sub-cache tier. It lives in
// shared so the reader can request a batch and the cache can serve it without an
// import cycle.
type V8SectionKey struct {
	Name    string
	TocType uint32
	SubType uint32
}
