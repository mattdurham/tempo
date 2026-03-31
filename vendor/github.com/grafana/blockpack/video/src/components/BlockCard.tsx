import React from "react";
import { interpolate, spring } from "remotion";
import type { Block } from "../data/blocks";
import { STAGES, getStageStartFrame } from "../data/pipeline";

type Props = {
  block: Block;
  stageIndex: number;
  frame: number;
};

function getPruneStageIndex(blockId: string): number {
  for (let i = 0; i < STAGES.length; i++) {
    if (STAGES[i].prunedBlocks.includes(blockId)) return i;
  }
  return -1;
}

function getPruneReason(blockId: string): string {
  for (const stage of STAGES) {
    if (stage.prunedBlocks.includes(blockId)) return stage.pruneReason;
  }
  return "";
}

export function BlockCard({ block, stageIndex, frame }: Props) {
  const pruneStageIdx = getPruneStageIndex(block.id);
  const isPruned = pruneStageIdx !== -1 && stageIndex >= pruneStageIdx;
  const isBeingPruned = stageIndex === pruneStageIdx;
  const pruneReason = getPruneReason(block.id);

  const showServices = stageIndex >= 1;
  const showDuration = stageIndex >= 3;
  const showSla = stageIndex >= 4;

  // Animate opacity when being pruned
  let opacity = 1;
  if (isPruned && pruneStageIdx !== -1) {
    const pruneStartFrame = getStageStartFrame(pruneStageIdx);
    const elapsed = frame - pruneStartFrame;
    if (isBeingPruned) {
      const springVal = spring({
        frame: elapsed,
        fps: 30,
        config: { damping: 20, stiffness: 80 },
      });
      opacity = interpolate(springVal, [0, 1], [1, 0.35]);
    } else {
      opacity = 0.35;
    }
  }

  const dotColor = isPruned ? "#EF4444" : stageIndex > 0 ? "#22C55E" : "#9CA3AF";
  const borderColor = isPruned ? "#FCA5A5" : stageIndex > 0 ? "#86EFAC" : "#E5E7EB";
  const bgColor = isPruned ? "#FFF5F5" : "#FFFFFF";
  const leftBorderColor = isPruned ? "#EF4444" : stageIndex > 0 ? "#22C55E" : "#E5E7EB";

  return (
    <div
      style={{
        height: 62,
        width: "100%",
        background: bgColor,
        border: `1px solid ${borderColor}`,
        borderLeft: `4px solid ${leftBorderColor}`,
        borderRadius: 6,
        display: "flex",
        alignItems: "center",
        padding: "0 10px",
        boxSizing: "border-box",
        opacity,
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
        gap: 8,
        overflow: "hidden",
      }}
    >
      {/* Dot */}
      <div
        style={{
          width: 10,
          height: 10,
          borderRadius: "50%",
          background: dotColor,
          flexShrink: 0,
        }}
      />

      {/* Block ID + file */}
      <div style={{ flexShrink: 0, minWidth: 80 }}>
        <div style={{ fontSize: 12, fontWeight: 700, color: "#111827", fontFamily: "monospace" }}>
          {block.id}
        </div>
        <div style={{ fontSize: 10, color: "#6B7280" }}>
          file: {block.file}
        </div>
      </div>

      {/* Metadata pills */}
      <div
        style={{
          flex: 1,
          display: "flex",
          flexDirection: "column",
          gap: 2,
          overflow: "hidden",
        }}
      >
        {/* Services */}
        <div style={{ fontSize: 10, color: "#6B7280", display: "flex", gap: 4, alignItems: "center", flexWrap: "nowrap" }}>
          <span style={{ color: "#9CA3AF", fontWeight: 500 }}>svc:</span>
          {showServices ? (
            block.services.map((s) => (
              <span
                key={s}
                style={{
                  background: "#EFF6FF",
                  border: "1px solid #BFDBFE",
                  borderRadius: 4,
                  padding: "0 4px",
                  color: "#1D4ED8",
                  fontSize: 10,
                }}
              >
                {s}
              </span>
            ))
          ) : (
            <span style={{ color: "#D1D5DB" }}>···</span>
          )}
        </div>

        {/* Duration + SLA */}
        <div style={{ fontSize: 10, color: "#6B7280", display: "flex", gap: 8 }}>
          <span>
            <span style={{ color: "#9CA3AF", fontWeight: 500 }}>dur: </span>
            {showDuration ? (
              <span style={{ color: "#374151" }}>{block.durMin}–{block.durMax}ms</span>
            ) : (
              <span style={{ color: "#D1D5DB" }}>···</span>
            )}
          </span>
          <span>
            <span style={{ color: "#9CA3AF", fontWeight: 500 }}>sla: </span>
            {showSla ? (
              block.sla.map((v) => (
                <span
                  key={v}
                  style={{
                    background: v === "high" ? "#FEF3C7" : "#F3F4F6",
                    border: `1px solid ${v === "high" ? "#FCD34D" : "#E5E7EB"}`,
                    borderRadius: 4,
                    padding: "0 4px",
                    color: v === "high" ? "#92400E" : "#6B7280",
                    fontSize: 10,
                    marginRight: 2,
                  }}
                >
                  {v}
                </span>
              ))
            ) : (
              <span style={{ color: "#D1D5DB" }}>···</span>
            )}
          </span>
        </div>
      </div>

      {/* Status badge */}
      <div style={{ flexShrink: 0, textAlign: "right" }}>
        {isPruned ? (
          <div
            style={{
              background: "#FEE2E2",
              border: "1px solid #FCA5A5",
              borderRadius: 6,
              padding: "2px 6px",
              fontSize: 9,
              color: "#991B1B",
              maxWidth: 100,
              lineHeight: 1.4,
            }}
          >
            <div style={{ fontWeight: 700 }}>✗ pruned</div>
            <div style={{ color: "#B91C1C" }}>{pruneReason}</div>
          </div>
        ) : stageIndex >= STAGES.length - 1 && block.matchingSpans > 0 ? (
          <div
            style={{
              background: "#F0FDF4",
              border: "1px solid #86EFAC",
              borderRadius: 6,
              padding: "2px 8px",
              fontSize: 10,
              color: "#166534",
              fontWeight: 700,
            }}
          >
            ✓ {block.matchingSpans} spans
          </div>
        ) : stageIndex > 0 && !isPruned ? (
          <div
            style={{
              background: "#F0FDF4",
              border: "1px solid #86EFAC",
              borderRadius: 6,
              padding: "2px 8px",
              fontSize: 10,
              color: "#166534",
              fontWeight: 700,
            }}
          >
            ✓ kept
          </div>
        ) : null}
      </div>
    </div>
  );
}
