package embedder

// NOTE: Any changes to this file must be reflected in the corresponding specs.md or NOTES.md.

import (
	"fmt"

	"github.com/hybridgroup/yzma/pkg/llama"
)

// yzmaBackend implements Backend using the yzma llama.cpp Go bindings.
// It requires a loaded llama.cpp shared library and a GGUF model file.
type yzmaBackend struct {
	model llama.Model
	lctx  llama.Context
	vocab llama.Vocab
	nEmbd int32
}

// newYzmaBackend loads the llama.cpp library and GGUF model, initializing an embedding context.
func newYzmaBackend(cfg Config) (*yzmaBackend, error) {
	if err := llama.Load(cfg.LibPath); err != nil {
		return nil, fmt.Errorf("embedder: load llama library %q: %w", cfg.LibPath, err)
	}

	llama.Init()

	model, err := llama.ModelLoadFromFile(cfg.ModelPath, llama.ModelDefaultParams())
	if err != nil {
		return nil, fmt.Errorf("embedder: load model %q: %w", cfg.ModelPath, err)
	}
	if model == 0 {
		return nil, fmt.Errorf("embedder: model load returned null for %q", cfg.ModelPath)
	}

	ctxParams := llama.ContextDefaultParams()
	ctxParams.NCtx = defaultContextSize
	ctxParams.NBatch = uint32(cfg.BatchSize) //nolint:gosec // batch size bounded by config validation
	ctxParams.Embeddings = 1

	lctx, err := llama.InitFromModel(model, ctxParams)
	if err != nil {
		_ = llama.ModelFree(model)
		return nil, fmt.Errorf("embedder: init context: %w", err)
	}

	vocab := llama.ModelGetVocab(model)
	nEmbd := llama.ModelNEmbd(model)

	return &yzmaBackend{
		model: model,
		lctx:  lctx,
		vocab: vocab,
		nEmbd: nEmbd,
	}, nil
}

// Dim returns the embedding dimension reported by the loaded model.
func (b *yzmaBackend) Dim() int { return int(b.nEmbd) }

// Embed encodes a single text string into an L2-normalized float32 vector.
func (b *yzmaBackend) Embed(text string) ([]float32, error) {
	vecs, err := b.EmbedBatch([]string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("embedder: Embed returned no vectors")
	}
	return vecs[0], nil
}

// EmbedBatch encodes each text sequentially, returning one L2-normalized vector per text.
func (b *yzmaBackend) EmbedBatch(texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		vec, err := b.embedOne(text)
		if err != nil {
			return nil, fmt.Errorf("embedder: embed[%d]: %w", i, err)
		}
		results[i] = vec
	}
	return results, nil
}

// Close frees model and context resources.
func (b *yzmaBackend) Close() {
	if b.lctx != 0 {
		_ = llama.Free(b.lctx)
		b.lctx = 0
	}
	if b.model != 0 {
		_ = llama.ModelFree(b.model)
		b.model = 0
	}
}

// embedOne runs inference for a single text and returns the L2-normalized embedding.
func (b *yzmaBackend) embedOne(text string) ([]float32, error) {
	tokens := llama.Tokenize(b.vocab, text, true, true)
	if len(tokens) == 0 {
		return make([]float32, b.nEmbd), nil
	}

	batch := llama.BatchGetOne(tokens)
	ret, err := llama.Decode(b.lctx, batch)
	if err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if ret != 0 {
		return nil, fmt.Errorf("decode returned %d", ret)
	}

	vec, err := llama.GetEmbeddingsSeq(b.lctx, 0, b.nEmbd)
	if err != nil {
		return nil, fmt.Errorf("get embeddings: %w", err)
	}

	return l2Normalize(vec), nil
}
