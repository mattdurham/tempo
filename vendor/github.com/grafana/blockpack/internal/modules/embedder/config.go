package embedder

// Config is a blockpack data type.
type Config struct {
	LibPath       string
	ModelPath     string
	ModelURL      string
	Fields        []EmbeddingField
	BatchSize     int
	MaxTextLength int
	AutoDownload  bool
}
