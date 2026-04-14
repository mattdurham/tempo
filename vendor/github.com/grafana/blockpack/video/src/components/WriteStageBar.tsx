import React from "react";
import { spring, interpolate } from "remotion";
import { WRITE_STAGES, getWriteStageStartFrame } from "../data/writepath";

type Props = {
  stageIndex: number;
  frame: number;
};

export function WriteStageBar({ stageIndex, frame }: Props) {
  const stage =
    stageIndex >= 0 && stageIndex < WRITE_STAGES.length
      ? WRITE_STAGES[stageIndex]
      : null;
  const label = stage?.label ?? "Initializing...";
  const description = stage?.description ?? "Buffering incoming spans";

  const stageStartFrame = stageIndex >= 0 ? getWriteStageStartFrame(stageIndex) : 0;
  const elapsed = frame - stageStartFrame;
  const fadeIn = spring({
    frame: elapsed,
    fps: 30,
    config: { damping: 24, stiffness: 120 },
  });
  const labelOpacity = interpolate(fadeIn, [0, 1], [0, 1]);

  return (
    <div
      style={{
        height: 80,
        background: "#FFFFFF",
        borderTop: "1px solid #E5E7EB",
        display: "flex",
        alignItems: "center",
        padding: "0 24px",
        boxSizing: "border-box",
        gap: 24,
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
      }}
    >
      {/* Stage progress dots */}
      <div style={{ display: "flex", gap: 6, flexShrink: 0 }}>
        {WRITE_STAGES.map((s, i) => (
          <div
            key={s.id}
            style={{
              width: 8,
              height: 8,
              borderRadius: "50%",
              background: i <= stageIndex ? "#F59E0B" : "#E5E7EB",
              border:
                i === stageIndex
                  ? "2px solid #FCD34D"
                  : "2px solid transparent",
              boxSizing: "border-box",
              transition: "none",
            }}
          />
        ))}
      </div>

      {/* Stage text */}
      <div style={{ flex: 1, opacity: labelOpacity }}>
        <div
          style={{
            fontSize: 15,
            fontWeight: 700,
            color: "#111827",
            lineHeight: 1.3,
          }}
        >
          {label}
        </div>
        <div
          style={{
            fontSize: 12,
            color: "#6B7280",
            lineHeight: 1.4,
          }}
        >
          {description}
        </div>
      </div>

      {/* Right label */}
      <div
        style={{
          flexShrink: 0,
          fontSize: 11,
          color: "#9CA3AF",
          fontFamily: "monospace",
          letterSpacing: "0.03em",
        }}
      >
        blockpack
      </div>
    </div>
  );
}
