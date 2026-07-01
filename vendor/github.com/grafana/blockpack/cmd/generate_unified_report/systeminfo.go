package main

type SystemInfo struct {
	CPUModel  string  `json:"cpuModel"`
	OS        string  `json:"os"`
	GoVersion string  `json:"goVersion"`
	RAM       float64 `json:"ram"`
}
