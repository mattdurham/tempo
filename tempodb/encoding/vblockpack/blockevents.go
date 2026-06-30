package vblockpack

// blockObjectKey returns the backend object key for a block's data file:
// "<tenant>/<block-id>/data.blockpack". This is the stable identifier the value
// index write path (NOTE-VI-042) stamps on each indexed (column, span) entry so
// the querier can open the source block from an index hit.
func blockObjectKey(tenantID, blockID string) string {
	return tenantID + "/" + blockID + "/" + DataFileName
}
