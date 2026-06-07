package embedder

type teiRequest struct {
	Inputs    []string `json:"inputs"`
	Normalize bool     `json:"normalize"`
}
