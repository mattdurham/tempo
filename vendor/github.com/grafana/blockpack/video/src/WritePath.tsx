import React from "react";
import { AbsoluteFill, useCurrentFrame } from "remotion";
import { WriteStageList } from "./components/WriteStageList";
import { FileStructure } from "./components/FileStructure";
import { WriteStageBar } from "./components/WriteStageBar";
import { getWriteStageIndex } from "./data/writepath";

export function WritePath() {
  const frame = useCurrentFrame();
  const stageIndex = getWriteStageIndex(frame);

  return (
    <AbsoluteFill
      style={{
        background: "#F8FAFC",
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Write bar — 50px */}
      <div
        style={{
          height: 50,
          flexShrink: 0,
          background: "#FFFFFF",
          borderBottom: "1px solid #E5E7EB",
          display: "flex",
          alignItems: "center",
          padding: "0 20px",
          gap: 12,
          boxSizing: "border-box",
        }}
      >
        <div
          style={{
            fontSize: 10,
            fontWeight: 700,
            color: "#9CA3AF",
            letterSpacing: "0.1em",
            textTransform: "uppercase",
            flexShrink: 0,
          }}
        >
          Write Path
        </div>
        <div
          style={{
            fontFamily: "monospace",
            fontSize: 13,
            color: "#1F2937",
            overflow: "hidden",
            whiteSpace: "nowrap",
            textOverflow: "ellipsis",
          }}
        >
          ~2000 spans{"  "}→{"  "}blockpack file
        </div>
        <div style={{ flex: 1 }} />
        <div
          style={{
            fontSize: 11,
            color: "#CBD5E0",
            fontFamily: "monospace",
            flexShrink: 0,
          }}
        >
          blockpack
        </div>
      </div>

      {/* Main area — 590px */}
      <div
        style={{
          height: 590,
          flexShrink: 0,
          display: "flex",
          flexDirection: "row",
        }}
      >
        {/* Left: Write Stage List (42%) */}
        <div
          style={{
            width: "42%",
            height: "100%",
            overflow: "hidden",
          }}
        >
          <WriteStageList stageIndex={stageIndex} />
        </div>

        {/* Divider */}
        <div
          style={{
            width: 1,
            height: "100%",
            background: "#E5E7EB",
            flexShrink: 0,
          }}
        />

        {/* Right: File Structure (58%) */}
        <div
          style={{
            width: "58%",
            height: "100%",
            overflow: "hidden",
          }}
        >
          <FileStructure stageIndex={stageIndex} frame={frame} />
        </div>
      </div>

      {/* Stage bar — 80px */}
      <div style={{ height: 80, flexShrink: 0 }}>
        <WriteStageBar stageIndex={stageIndex} frame={frame} />
      </div>
    </AbsoluteFill>
  );
}
