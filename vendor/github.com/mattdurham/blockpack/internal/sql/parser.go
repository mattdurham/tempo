package sql

import (
	"fmt"
	"regexp"

	"github.com/auxten/postgresql-parser/pkg/sql/parser"
	"github.com/auxten/postgresql-parser/pkg/sql/sem/tree"
)

const colonPlaceholder = "__COLON__"

// ParseSQL parses a SQL query and validates it's a supported SELECT statement.
// Supports: SELECT * FROM spans [WHERE <conditions>] [GROUP BY <fields>] [LIMIT <number>]
func ParseSQL(sqlQuery string) (*tree.Select, error) {
	// Replace colons with placeholder to work around PostgreSQL parser limitations
	// PostgreSQL uses : for parameter placeholders, but we use it for intrinsic fields
	sqlQuery = replaceColonsInIdentifiers(sqlQuery, colonPlaceholder)

	// Parse the SQL query
	stmts, err := parser.Parse(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Must be exactly one statement
	if len(stmts) == 0 {
		return nil, fmt.Errorf("empty SQL query")
	}
	if len(stmts) > 1 {
		return nil, fmt.Errorf("only single statements supported, got %d statements", len(stmts))
	}

	// Must be a SELECT statement
	selectStmt, ok := stmts[0].AST.(*tree.Select)
	if !ok {
		return nil, fmt.Errorf("only SELECT statements supported, got: %T", stmts[0].AST)
	}

	// Validate the SELECT statement structure
	if err := ValidateSelectStatement(selectStmt); err != nil {
		return nil, fmt.Errorf("unsupported SQL: %w", err)
	}

	return selectStmt, nil
}

// ValidateSelectStatement validates that a SELECT statement conforms to our supported subset.
// Supported: SELECT * FROM spans [WHERE <conditions>] [LIMIT <number>]
func ValidateSelectStatement(stmt *tree.Select) error {
	// Get the SelectClause (handles UNION, etc.)
	selectClause, ok := stmt.Select.(*tree.SelectClause)
	if !ok {
		return fmt.Errorf("only basic SELECT supported, got: %T", stmt.Select)
	}

	// Validate: Must have FROM clause
	if len(selectClause.From.Tables) == 0 {
		return fmt.Errorf("FROM clause required")
	}

	// Validate: Must have exactly one table
	if len(selectClause.From.Tables) > 1 {
		return fmt.Errorf("only single table queries supported, got %d tables", len(selectClause.From.Tables))
	}

	// Validate: Table must be supported (no joins)
	if err := validateTable(selectClause.From.Tables[0]); err != nil {
		return err
	}

	// Check if this is a CASE expression query
	hasCaseExprs := hasCaseExpressions(selectClause.Exprs)

	// Check if this is an aggregation query
	hasAggregates := hasAggregateFunctions(selectClause.Exprs) || selectClause.GroupBy != nil

	// Validate all CASE expressions
	if hasCaseExprs {
		if err := validateCaseExpressions(selectClause.Exprs); err != nil {
			return err
		}
	}
	// For regular filter queries (not CASE expressions or aggregates), validate SELECT *
	if !hasCaseExprs && !hasAggregates {
		if err := validateSelectStar(selectClause.Exprs); err != nil {
			return err
		}
	}

	// WHERE clause is optional (if not present, returns all spans)
	// But if it contains CASE expressions, validate them
	if selectClause.Where != nil {
		if err := validateCaseExprsInExpr(selectClause.Where.Expr); err != nil {
			return err
		}
	}

	// Validate: No HAVING
	if selectClause.Having != nil {
		return fmt.Errorf("HAVING is not supported")
	}

	// Validate: No DISTINCT
	if selectClause.Distinct {
		return fmt.Errorf("DISTINCT is not supported")
	}

	// Validate: No window functions
	if len(selectClause.Window) > 0 {
		return fmt.Errorf("WINDOW functions are not supported")
	}

	// Validate: No ORDER BY
	if len(stmt.OrderBy) > 0 {
		return fmt.Errorf("ORDER BY is not supported")
	}

	return nil
}

// validateSelectStar checks if SELECT clause is "SELECT *"
func validateSelectStar(exprs tree.SelectExprs) error {
	if len(exprs) != 1 {
		return fmt.Errorf("only SELECT * supported for filter queries, got %d expressions", len(exprs))
	}

	expr := exprs[0]
	if expr.Expr == nil {
		return fmt.Errorf("invalid SELECT expression")
	}

	// Check if it's a star expression (SELECT *)
	if _, ok := expr.Expr.(tree.UnqualifiedStar); !ok {
		return fmt.Errorf("only SELECT * supported for filter queries")
	}

	return nil
}

// validateTable checks if the table is supported for SQL queries.
func validateTable(tableExpr tree.TableExpr) error {
	tableName, ok := tableExpr.(*tree.AliasedTableExpr)
	if !ok {
		return fmt.Errorf("unsupported table expression: %T", tableExpr)
	}

	nameExpr, ok := tableName.Expr.(*tree.TableName)
	if !ok {
		return fmt.Errorf("unsupported table name expression: %T", tableName.Expr)
	}

	switch nameExpr.Table() {
	case "spans", "realm_files", "realm_metadata":
		return nil
	default:
		return fmt.Errorf(
			"only 'spans', 'realm_files', or 'realm_metadata' tables supported, got: %s",
			nameExpr.Table(),
		)
	}
}

// hasCaseExpressions checks if any SELECT expression is or contains a CASE expression
func hasCaseExpressions(exprs tree.SelectExprs) bool {
	for _, expr := range exprs {
		if containsCaseExpr(expr.Expr) {
			return true
		}
	}
	return false
}

// containsCaseExpr recursively checks if an expression contains a CASE expression
func containsCaseExpr(expr tree.Expr) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *tree.CaseExpr:
		return true
	case *tree.FuncExpr:
		// Check function arguments
		for _, arg := range e.Exprs {
			if containsCaseExpr(arg) {
				return true
			}
		}
		return false
	case *tree.ParenExpr:
		return containsCaseExpr(e.Expr)
	case *tree.BinaryExpr:
		return containsCaseExpr(e.Left) || containsCaseExpr(e.Right)
	case *tree.UnaryExpr:
		return containsCaseExpr(e.Expr)
	case *tree.ComparisonExpr:
		return containsCaseExpr(e.Left) || containsCaseExpr(e.Right)
	case *tree.AndExpr:
		return containsCaseExpr(e.Left) || containsCaseExpr(e.Right)
	case *tree.OrExpr:
		return containsCaseExpr(e.Left) || containsCaseExpr(e.Right)
	case *tree.NotExpr:
		return containsCaseExpr(e.Expr)
	default:
		return false
	}
}

// validateCaseExpressions validates all CASE expressions in SELECT clause
// This now recursively searches for CASE expressions in nested function calls
func validateCaseExpressions(exprs tree.SelectExprs) error {
	for _, expr := range exprs {
		if err := validateCaseExprsInExpr(expr.Expr); err != nil {
			return err
		}
	}
	return nil
}

// validateCaseExprsInExpr recursively validates all CASE expressions in an expression tree
func validateCaseExprsInExpr(expr tree.Expr) error {
	if expr == nil {
		return nil
	}

	switch e := expr.(type) {
	case *tree.CaseExpr:
		return validateCaseExpr(e)
	case *tree.FuncExpr:
		// Recursively validate CASE expressions in function arguments
		for _, arg := range e.Exprs {
			if err := validateCaseExprsInExpr(arg); err != nil {
				return err
			}
		}
		return nil
	case *tree.AndExpr:
		if err := validateCaseExprsInExpr(e.Left); err != nil {
			return err
		}
		return validateCaseExprsInExpr(e.Right)
	case *tree.OrExpr:
		if err := validateCaseExprsInExpr(e.Left); err != nil {
			return err
		}
		return validateCaseExprsInExpr(e.Right)
	case *tree.NotExpr:
		return validateCaseExprsInExpr(e.Expr)
	case *tree.ParenExpr:
		return validateCaseExprsInExpr(e.Expr)
	case *tree.ComparisonExpr:
		if err := validateCaseExprsInExpr(e.Left); err != nil {
			return err
		}
		return validateCaseExprsInExpr(e.Right)
	case *tree.BinaryExpr:
		if err := validateCaseExprsInExpr(e.Left); err != nil {
			return err
		}
		return validateCaseExprsInExpr(e.Right)
	case *tree.UnaryExpr:
		return validateCaseExprsInExpr(e.Expr)
	}

	return nil
}

// isAliasInGroupBy checks if an alias name appears in the GROUP BY clause
// extractCaseReturnType determines the type returned by all branches of a CASE expression
// Returns: "string", "int64", "float64", "bool", or error if types are inconsistent
func extractCaseReturnType(caseExpr *tree.CaseExpr) (string, error) {
	if caseExpr == nil {
		return "", fmt.Errorf("nil CASE expression")
	}

	var firstConcreteType string

	// Check all WHEN...THEN branches
	for i, when := range caseExpr.Whens {
		thenType := getExprType(when.Val)
		if i == 0 {
			firstConcreteType = thenType
		} else if !typesCompatible(thenType, firstConcreteType) {
			return "", fmt.Errorf("CASE branches must return consistent types: first branch returns %s but branch %d returns %s", firstConcreteType, i+1, thenType)
		}
		// Update firstConcreteType if we found a concrete type
		if firstConcreteType == TypeUnknown && thenType != TypeUnknown {
			firstConcreteType = thenType
		}
	}

	// Check ELSE branch
	if caseExpr.Else == nil {
		return "", fmt.Errorf("CASE expression must have ELSE clause")
	}
	elseType := getExprType(caseExpr.Else)
	if !typesCompatible(elseType, firstConcreteType) {
		return "", fmt.Errorf(
			"CASE branches must return consistent types: THEN returns %s but ELSE returns %s",
			firstConcreteType,
			elseType,
		)
	}
	// Update firstConcreteType if ELSE has a concrete type
	if firstConcreteType == TypeUnknown && elseType != TypeUnknown {
		firstConcreteType = elseType
	}

	return firstConcreteType, nil
}

// typesCompatible checks if two types are compatible (same type or one is TypeUnknown)
func typesCompatible(type1, type2 string) bool {
	if type1 == type2 {
		return true
	}
	// TypeUnknown is compatible with any type
	if type1 == TypeUnknown || type2 == TypeUnknown {
		return true
	}
	return false
}

// getExprType returns the type of an expression ("string", "int64", "float64", "bool", TypeUnknown)
func getExprType(expr tree.Expr) string {
	switch e := expr.(type) {
	case *tree.StrVal:
		return "string"
	case *tree.NumVal:
		// Try to determine if it's a float by checking if it can be parsed as int64
		if _, err := e.AsInt64(); err != nil {
			return "float64"
		}
		return "int64"
	case *tree.DBool:
		return "bool"
	case *tree.CaseExpr:
		// Recursive: get the type of the CASE expression
		typ, _ := extractCaseReturnType(e)
		return typ
	default:
		return TypeUnknown
	}
}

// validateCaseExpr validates a CASE expression
func validateCaseExpr(caseExpr *tree.CaseExpr) error {
	// Verify it's a searched CASE (not simple CASE)
	if caseExpr.Expr != nil {
		return fmt.Errorf("simple CASE syntax not supported, use searched CASE (CASE WHEN condition THEN value)")
	}

	// Must have at least one WHEN clause
	if len(caseExpr.Whens) == 0 {
		return fmt.Errorf("CASE must have at least one WHEN clause")
	}

	// Must have ELSE clause
	if caseExpr.Else == nil {
		return fmt.Errorf("CASE expression must have ELSE clause")
	}

	// Validate type consistency
	_, err := extractCaseReturnType(caseExpr)
	if err != nil {
		return err
	}

	return nil
}

// replaceColonsInIdentifiers replaces colons within quoted identifiers with a placeholder
// to work around PostgreSQL parser limitations (: is used for parameter placeholders)
func replaceColonsInIdentifiers(query string, placeholder string) string {
	// Match quoted identifiers like "span:name"
	re := regexp.MustCompile(`"([^"]*):([^"]*)"`)
	return re.ReplaceAllStringFunc(query, func(match string) string {
		// Replace : with placeholder inside the quotes
		return regexp.MustCompile(`:`).ReplaceAllString(match, placeholder)
	})
}
