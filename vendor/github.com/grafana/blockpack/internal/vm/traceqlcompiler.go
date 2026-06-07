package vm

type traceqlCompiler struct {
	program  *Program
	embedder TextEmbedder
	opts     CompileOptions
}
