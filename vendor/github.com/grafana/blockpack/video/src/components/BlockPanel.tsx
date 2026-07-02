import React from "react";
import { BLOCKS } from "../data/blocks";
import { STAGES, getPrunedBlockIds } from "../data/pipeline";
import { BlockCard } from "./BlockCard";

type Props = {
  stageIndex: number;
  frame: number;
};

function getSummaryText(stageIndex: number): string {
  if (stageIndex < 0) return "8 blocks";
  const pruned = getPrunedBlockIds(stageIndex);
  const remaining = BLOCKS.length - pruned.size;
  if (remaining === BLOCKS.length) return "8 blocks";
  return `${remaining} of ${BLOCKS.length} remain`;
}

export function BlockPanel({ stageIndex, frame }: Props) {
  const summary = getSummaryText(stageIndex);
  const prunedCount = stageIndex >= 0 ? getPrunedBlockIds(stageIndex).size : 0;
  const stageLabel =
    stageIndex >= 0 && stageIndex < STAGES.length
      ? STAGES[stageIndex].label
      : "";

  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        padding: "10px 12px 6px 12px",
        boxSizing: "border-box",
        gap: 4,
        overflow: "hidden",
      }}
    >
      {/* Summary header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 4,
          flexShrink: 0,
        }}
      >
        <div
          style={{
            fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
            fontSize: 12,
            fontWeight: 700,
            color: "#374151",
            letterSpacing: "0.04em",
            textTransform: "uppercase",
          }}
        >
          {summary}
        </div>
        {prunedCount > 0 && (
          <div
            style={{
              fontSize: 10,
              color: "#EF4444",
              fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
              fontWeight: 500,
            }}
          >
            {prunedCount} pruned
          </div>
        )}
        {stageLabel && (
          <div
            style={{
              fontSize: 10,
              color: "#9CA3AF",
              fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
            }}
          >
            {stageLabel}
          </div>
        )}
      </div>

      {/* Block cards */}
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          gap: 5,
          flex: 1,
          overflow: "hidden",
        }}
      >
        {BLOCKS.map((block) => (
          <BlockCard
            key={block.id}
            block={block}
            stageIndex={stageIndex}
            frame={frame}
          />
        ))}
      </div>
    </div>
  );
}
