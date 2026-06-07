package shared

// TextEmbedder is a blockpack data type.
type TextEmbedder interface {
	Embed(text string) ([]float32, error)
	EmbedBatch(texts []string) ([][]float32, error)
}
