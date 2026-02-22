#!/usr/bin/env python3
import re

# Read the original file
with open('/tmp/original_blockpack_executor.go', 'r') as f:
    content = f.read()

# Split into lines for processing
lines = content.split('\n')

# Helper to extract function by name
def extract_function(lines, func_pattern):
    """Extract a complete function by pattern"""
    result = []
    in_function = False
    brace_count = 0
    
    for i, line in enumerate(lines):
        if re.match(func_pattern, line):
            in_function = True
            result.append(line)
            if '{' in line:
                brace_count += line.count('{') - line.count('}')
            continue
            
        if in_function:
            result.append(line)
            brace_count += line.count('{') - line.count('}')
            if brace_count == 0 and result:
                break
                
    return '\n'.join(result)

print("Extracting functions systematically...")

# Extract aggregation functions
agg_functions = [
    r'^func updateBucketCount\(',
    r'^func updateBucketSum\(',
    r'^func updateBucketMin\(',
    r'^func updateBucketMax\(',
    r'^func finalizeAvg\(',
    r'^func finalizeRate\(',
    r'^func initBucketQuantile\(',
    r'^func updateBucketQuantile\(',
    r'^func extractBucketQuantile\(',
    r'^func updateBucketHistogram\(',
    r'^func updateBucketStddev\(',
    r'^func finalizeStddev\(',
    r'^func \(e \*BlockpackExecutor\) executeAggregationQuery\(',
    r'^func processAggregationRow\(',
    r'^func extractGroupKeyValues\(',
    r'^func extractGroupKeyValuesReuse\(',
    r'^func concatenateGroupKey\(',
    r'^func concatenateGroupKeyFromSlice\(',
    r'^func formatValueAsString\(',
    r'^func updateAggregatesForRow\(',
    r'^func convertToFloat64\(',
    r'^func buildAggregateResult\(',
    r'^func buildDenseTimeSeriesResult\(',
    r'^func denseBucketCount\(',
    r'^func extractDenseAttributeGroups\(',
    r'^func denseAttributeGroupKey\(',
    r'^func buildDenseAggregateRows\(',
    r'^func denseAttributeGroupValues\(',
    r'^func denseFullGroupKey\(',
    r'^func emptyAggBucket\(',
    r'^func buildDenseAggregateValues\(',
    r'^func aggregationOutputName\(',
    r'^func aggregationValue\(',
    r'^func sortDenseAggregateRows\(',
    r'^func buildDenseAggregateResult\(',
]

agg_code = []
for pattern in agg_functions:
    func = extract_function(lines, pattern)
    if func:
        agg_code.append(func)

with open('aggregation_executor.go', 'w') as f:
    f.write('''package executor

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/mattdurham/blockpack/internal/arena"
	blockpackio "github.com/mattdurham/blockpack/internal/blockio"
	"github.com/mattdurham/blockpack/internal/quantile"
	blockpack "github.com/mattdurham/blockpack/internal/types"
	"github.com/mattdurham/blockpack/internal/vm"
)

''')
    f.write('\n\n'.join(agg_code))
    
print("✓ aggregation_executor.go created")
