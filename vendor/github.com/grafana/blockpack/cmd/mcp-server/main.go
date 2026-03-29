// Package main is the entry point for the BlockPack MCP server.
package main

import (
	"fmt"
	"log"

	serv "github.com/grafana/blockpack/cmd/mcp-server/server"
	"github.com/grafana/blockpack/cmd/mcp-server/tools"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	// Initialize storage
	storage, err := serv.NewStorage()
	if err != nil {
		log.Fatalf("initialize storage: %v", err)
	}

	// Create MCP server
	mcpServer := server.NewMCPServer(
		"blockpack",
		"1.0.0",
	)

	// Register AI Assistant Support tools
	mcpServer.AddTool(tools.DocsTool(), tools.DocsHandler())
	mcpServer.AddTool(tools.ArchitectureTool(storage), tools.ArchitectureHandler(storage))
	mcpServer.AddTool(tools.RequirementsTool(storage), tools.RequirementsHandler(storage))
	mcpServer.AddTool(tools.PrecommitTool(storage), tools.PrecommitHandler(storage))

	// Register Developer Tooling tools
	mcpServer.AddTool(tools.AnalysisTool(storage), tools.AnalysisHandler(storage))
	mcpServer.AddTool(tools.FileLayoutTool(storage), tools.FileLayoutHandler(storage))
	mcpServer.AddTool(tools.BenchmarkTool(storage), tools.BenchmarkHandler(storage))
	mcpServer.AddTool(tools.ValidationTool(storage), tools.ValidationHandler(storage))
	mcpServer.AddTool(tools.DescribeFileTool(storage), tools.DescribeFileHandler(storage))
	mcpServer.AddTool(tools.DescribeDirectoryTool(storage), tools.DescribeDirectoryHandler(storage))

	// Register Conversion tools
	mcpServer.AddTool(tools.ConvertProtoTool(storage), tools.ConvertProtoHandler(storage))
	mcpServer.AddTool(tools.ConvertParquetTool(storage), tools.ConvertParquetHandler(storage))

	// Register Scan tool
	mcpServer.AddTool(tools.ScanDirectoryTool(storage), tools.ScanDirectoryHandler(storage))

	// Register Tempo query tool
	mcpServer.AddTool(tools.TempoQueryTool(storage), tools.TempoQueryHandler(storage))

	// Register module spec search tool
	mcpServer.AddTool(tools.SearchModulesTool(), tools.SearchModulesHandler())

	// Start stdio server
	log.Println("Starting BlockPack MCP server...")
	if err := server.ServeStdio(mcpServer); err != nil {
		fmt.Printf("Server error: %v\n", err)
	}
}
