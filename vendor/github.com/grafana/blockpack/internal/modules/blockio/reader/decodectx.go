package reader

type decodeCtx struct {
	intern  map[string]string
	scratch *[]byte
}
