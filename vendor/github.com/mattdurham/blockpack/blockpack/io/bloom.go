package ondiskio

import "hash/fnv"

// BOT: Can these be on the ColumnNameBloomFilter?
func addToColumnNameBloom(bloom *ColumnNameBloom, name string) {
	h1 := fnv.New32a()
	_, _ = h1.Write([]byte(name)) // hash.Write never errors
	hash1 := h1.Sum32()

	h2 := fnv.New32()
	_, _ = h2.Write([]byte(name)) // hash.Write never errors
	hash2 := h2.Sum32()

	setColumnNameBloomBit(bloom, hash1%columnNameBloomBits)
	setColumnNameBloomBit(bloom, hash2%columnNameBloomBits)
}

func setColumnNameBloomBit(bloom *ColumnNameBloom, pos uint32) {
	byteIdx := pos / 8
	bit := pos % 8
	bloom[byteIdx] |= 1 << bit
}
