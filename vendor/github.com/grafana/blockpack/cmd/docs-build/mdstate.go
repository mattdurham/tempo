package main

import "strings"

type mdState struct {
	sb          strings.Builder
	inCode      bool
	inTable     bool
	inParagraph bool
	skipSection bool
}
