# TESTS: sectioncache

## Coverage Goal: > 70%

## TestFilecacheAdapter_GetOrFetchFooter
Verifies footer key format: fileID+"/footer"+variant. Tests empty variant (V3 bare key),
V4/V5/V6/V8 variant strings. Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetOrFetchHeader
Verifies header key: fileID+"/header". Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetOrFetchV8TOC
Verifies V8 ToC key: fileID+"/v8/toc/dec". Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetOrFetchV8Section
Verifies null-byte key format: fileID+"\x00v8\x00"+tocType+"\x00"+subType+"\x00"+name.
Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetOrFetchV14Section
Verifies V14 section key: fileID+"/v14/sec/XX/dec" (hex-encoded section type). Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetOrFetchBloom
Verifies compact-header key for isV14=false ("/compact-header") and isV14=true
("/v14/compact-header"). Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetBlockColumns
Verifies block column key: fileID+"/block/"+blockIdx. Checks that the key matches
BlockColumnsKey(fileID, blockIdx). Verifies SPEC-SC-002, NOTE-SC-002.

## TestFilecacheAdapter_CacheBlockColumns
Verifies that CacheBlockColumns stores with the same key as GetBlockColumns. Verifies SPEC-SC-002.

## TestFilecacheAdapter_GetOrFetchIntrinsic
Verifies intrinsic key: fileID+"/intrinsic/"+name. Verifies SPEC-SC-002.

## TestNopSectionCache_AlwaysFetches
Verifies NopSectionCache calls fetch on every GetOrFetch* call. Verifies SPEC-SC-003.

## TestNopSectionCache_GetAlwaysMisses
Verifies NopSectionCache Get returns (nil, false, nil). Verifies SPEC-SC-003.

## TestBlockColumnsKey_MatchesFmtSprintf
Table-driven test: BlockColumnsKey and BlockColumnsKeyFast must produce identical output
for a range of blockIdx values (0, 1, 99, 100, 9999, MaxInt). Verifies NOTE-SC-002.
