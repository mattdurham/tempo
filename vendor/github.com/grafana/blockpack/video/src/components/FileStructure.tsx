import React from "react";
import { spring, interpolate } from "remotion";
import { WRITE_STAGES, getWriteStageStartFrame } from "../data/writepath";

type FileSectionDef = {
  id: string;
  label: string;
  hint: string;
};

const FILE_SECTIONS: FileSectionDef[] = [
  { id: "pending-spans", label: "Pending spans", hint: "~88B/span" },
  { id: "block-payloads", label: "Block payloads", hint: "columnar + Zstd" },
  { id: "kll", label: "KLL range sketches", hint: "~200–500B/block" },
  { id: "cms-hll-topk", label: "CMS + HLL + TopK", hint: "~720B/block" },
  { id: "bloom", label: "Bloom filters", hint: "~1B/entry/col" },
  { id: "intrinsics", label: "Intrinsic columns", hint: "8 core attributes" },
  {
    id: "trace-index",
    label: "Trace block index",
    hint: "sorted (traceID→block)",
  },
  { id: "fblm", label: "File-level bloom (FBLM)", hint: "~1B/entry" },
  { id: "header", label: "Header + footer", hint: "fixed size" },
];

/** Returns the stage index that first activates a given section id, or -1. */
function getActivatingStageIndex(sectionId: string): number {
  for (let i = 0; i < WRITE_STAGES.length; i++) {
    if (WRITE_STAGES[i].activeSection.includes(sectionId)) return i;
  }
  return -1;
}

type Props = {
  stageIndex: number;
  frame: number;
};

export function FileStructure({ stageIndex, frame }: Props) {
  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        display: "flex",
        flexDirection: "column",
        padding: "10px 16px 10px 12px",
        boxSizing: "border-box",
        gap: 5,
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
        File Structure
      </div>

      {FILE_SECTIONS.map((section) => {
        const activatingStage = getActivatingStageIndex(section.id);
        const isActive = activatingStage !== -1 && stageIndex === activatingStage;
        const isCompleted =
          activatingStage !== -1 && stageIndex > activatingStage;
        const isReached = isActive || isCompleted;

        // Compute fill progress via spring when first activated
        let fillProgress = 0;
        if (isCompleted) {
          fillProgress = 1;
        } else if (isActive && activatingStage !== -1) {
          const stageStart = getWriteStageStartFrame(activatingStage);
          const elapsed = frame - stageStart;
          const springVal = spring({
            frame: elapsed,
            fps: 30,
            config: { damping: 20, stiffness: 60 },
          });
          fillProgress = interpolate(springVal, [0, 1], [0, 1], {
            extrapolateRight: "clamp",
          });
        }

        const fillColor = isCompleted
          ? "#86EFAC"
          : isActive
            ? "#F59E0B"
            : "transparent";
        const borderColor = isCompleted
          ? "#86EFAC"
          : isActive
            ? "#F59E0B"
            : "#E5E7EB";
        const labelColor = isActive
          ? "#92400E"
          : isCompleted
            ? "#374151"
            : "#9CA3AF";
        const labelWeight = isActive ? 700 : isCompleted ? 500 : 400;

        return (
          <div
            key={section.id}
            style={{
              flexShrink: 0,
              display: "flex",
              alignItems: "center",
              gap: 10,
              height: 48,
              overflow: "hidden",
            }}
          >
            {/* Label column */}
            <div
              style={{
                width: 160,
                flexShrink: 0,
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
                {section.label}
              </div>
              <div
                style={{
                  fontSize: 10,
                  color: isReached ? "#9CA3AF" : "#D1D5DB",
                  whiteSpace: "nowrap",
                  lineHeight: 1.3,
                }}
              >
                {section.hint}
              </div>
            </div>

            {/* Fill bar track */}
            <div
              style={{
                flex: 1,
                height: 16,
                borderRadius: 4,
                background: "#F3F4F6",
                border: `1px solid ${borderColor}`,
                overflow: "hidden",
                position: "relative",
              }}
            >
              {/* Fill */}
              <div
                style={{
                  position: "absolute",
                  top: 0,
                  left: 0,
                  height: "100%",
                  width: `${fillProgress * 100}%`,
                  background: fillColor,
                  borderRadius: 4,
                }}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
}
