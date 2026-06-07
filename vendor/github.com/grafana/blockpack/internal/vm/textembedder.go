package vm

// TextEmbedder is a blockpack data type.
type TextEmbedder interface {
	Embed(text string) ([]float32, error)
}
