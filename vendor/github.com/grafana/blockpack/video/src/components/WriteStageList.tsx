import React from "react";
import { WRITE_STAGES } from "../data/writepath";

type Props = {
  stageIndex: number;
};

export function WriteStageList({ stageIndex }: Props) {
  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        padding: "10px 12px",
        boxSizing: "border-box",
        gap: 6,
        overflow: "hidden",
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
      }}
    >
      {/* Panel header */}
      <div
        style={{
          fontSize: 11,
          fontWeight: 700,
          color: "#9CA3AF",
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          marginBottom: 4,
          flexShrink: 0,
        }}
      >
        Write Pipeline
      </div>

      {/* Stage rows */}
      {WRITE_STAGES.map((stage, i) => {
        const isActive = i === stageIndex;
        const isCompleted = i < stageIndex;

        let markerColor: string;
        let markerText: string;
        if (isActive) {
          markerColor = "#F59E0B";
          markerText = "══►";
        } else if (isCompleted) {
          markerColor = "#22C55E";
          markerText = "✓";
        } else {
          markerColor = "#D1D5DB";
          markerText = "───";
        }

        const bg = isActive ? "#FFFBEB" : "#FFFFFF";
        const leftBorder = isActive
          ? "3px solid #F59E0B"
          : isCompleted
            ? "3px solid #86EFAC"
            : "3px solid transparent";
        const labelColor = isActive
          ? "#92400E"
          : isCompleted
            ? "#6B7280"
            : "#9CA3AF";
        const labelWeight = isActive ? 700 : 400;

        return (
          <div
            key={stage.id}
            style={{
              height: 52,
              flexShrink: 0,
              background: bg,
              borderLeft: leftBorder,
              borderRadius: 4,
              display: "flex",
              alignItems: "center",
              padding: "0 8px",
              boxSizing: "border-box",
              gap: 8,
              overflow: "hidden",
            }}
          >
            {/* Stage number chip */}
            <div
              style={{
                width: 18,
                height: 18,
                borderRadius: "50%",
                background: isActive
                  ? "#F59E0B"
                  : isCompleted
                    ? "#86EFAC"
                    : "#E5E7EB",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontSize: 9,
                fontWeight: 700,
                color: isActive ? "#FFFFFF" : isCompleted ? "#166534" : "#9CA3AF",
                flexShrink: 0,
              }}
            >
              {i + 1}
            </div>

            {/* Marker arrow / check */}
            <div
              style={{
                fontSize: isActive ? 10 : 11,
                color: markerColor,
                fontFamily: "monospace",
                flexShrink: 0,
                width: 28,
                textAlign: "center",
              }}
            >
              {markerText}
            </div>

            {/* Label + description */}
            <div
              style={{
                flex: 1,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  fontSize: 12,
                  fontWeight: labelWeight,
                  color: labelColor,
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  lineHeight: 1.3,
                }}
              >
                {stage.label}
              </div>
              {isActive && (
                <div
                  style={{
                    fontSize: 10,
                    color: "#B45309",
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    lineHeight: 1.3,
                    marginTop: 1,
                  }}
                >
                  {stage.description}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
