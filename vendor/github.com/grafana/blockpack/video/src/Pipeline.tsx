import React from "react";
import { AbsoluteFill, useCurrentFrame, useVideoConfig } from "remotion";
import { PredicateTree } from "./components/PredicateTree";
import { BlockPanel } from "./components/BlockPanel";
import { StageBar } from "./components/StageBar";
import { QUERY_STRING } from "./data/query";
import { STAGES, getActiveStageIndex } from "./data/pipeline";

export function Pipeline() {
  const frame = useCurrentFrame();
  useVideoConfig();

  const stageIndex = getActiveStageIndex(frame);
  const activePredicates =
    stageIndex >= 0 && stageIndex < STAGES.length
      ? STAGES[stageIndex].activePredicates
      : [];

  return (
    <AbsoluteFill
      style={{
        background: "#F8FAFC",
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
        display: "flex",
        flexDirection: "column",
      }}
    >
      {/* Query bar — 50px */}
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
          TraceQL
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
          {QUERY_STRING}
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
        {/* Left: Predicate Tree (55%) */}
        <div
          style={{
            width: "55%",
            height: "100%",
            overflow: "hidden",
          }}
        >
          <PredicateTree
            activePredicates={activePredicates}
            stageIndex={stageIndex}
          />
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

        {/* Right: Block Panel (45%) */}
        <div
          style={{
            width: "45%",
            height: "100%",
            overflow: "hidden",
          }}
        >
          <BlockPanel stageIndex={stageIndex} frame={frame} />
        </div>
      </div>

      {/* Stage bar — 80px */}
      <div style={{ height: 80, flexShrink: 0 }}>
        <StageBar stageIndex={stageIndex} frame={frame} />
      </div>
    </AbsoluteFill>
  );
}
