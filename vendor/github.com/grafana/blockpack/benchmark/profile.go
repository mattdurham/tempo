package benchmark

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

type benchProfiler struct {
	cpuFile           *os.File
	memFile           *os.File
	memProfileRateOld int
}

func startBenchProfiler(name string) (*benchProfiler, error) {
	dir := os.Getenv("BENCH_PROFILE_DIR")
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	enableCPU := os.Getenv("BENCH_PROFILE_CPU") != ""
	enableMem := os.Getenv("BENCH_PROFILE_MEM") != ""
	if !enableCPU && !enableMem {
		return nil, nil
	}

	profiler := &benchProfiler{}
	if enableCPU {
		cpuPath := filepath.Join(dir, name+".cpu.pprof")
		cpuFile, err := os.Create(cpuPath)
		if err != nil {
			return nil, err
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			return nil, err
		}
		profiler.cpuFile = cpuFile
	}

	if enableMem {
		memPath := filepath.Join(dir, name+".mem.pprof")
		memFile, err := os.Create(memPath)
		if err != nil {
			if profiler.cpuFile != nil {
				pprof.StopCPUProfile()
				_ = profiler.cpuFile.Close()
			}
			return nil, err
		}
		profiler.memFile = memFile
	}

	return profiler, nil
}

func startBenchProfilerPhase(name, phase string) (*benchProfiler, error) {
	if !shouldProfilePhase(phase) {
		return nil, nil
	}
	profiler, err := startBenchProfiler(name)
	if err != nil {
		return nil, err
	}
	if profiler != nil && profiler.memFile != nil && phase == "queries" {
		profiler.memProfileRateOld = runtime.MemProfileRate
		runtime.MemProfileRate = 1
		runtime.GC()
	}
	return profiler, nil
}

func shouldProfilePhase(phase string) bool {
	cfg := os.Getenv("BENCH_PROFILE_PHASE")
	if cfg == "" || cfg == "all" {
		return phase == "all"
	}
	return cfg == phase
}

func (p *benchProfiler) Stop() error {
	if p == nil {
		return nil
	}
	if p.cpuFile != nil {
		pprof.StopCPUProfile()
		if err := p.cpuFile.Close(); err != nil {
			return err
		}
	}
	if p.memFile != nil {
		runtime.GC()
		if err := pprof.WriteHeapProfile(p.memFile); err != nil {
			_ = p.memFile.Close()
			return err
		}
		if err := p.memFile.Close(); err != nil {
			return err
		}
		runtime.MemProfileRate = p.memProfileRateOld
	}
	return nil
}
