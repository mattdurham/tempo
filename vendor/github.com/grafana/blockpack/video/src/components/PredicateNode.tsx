import React from "react";
import { Handle, Position } from "@xyflow/react";
import type { NodeProps, Node } from "@xyflow/react";
import type { PredicateNodeData, NodeStatus } from "../data/query";

export type PredicateNodeType = Node<PredicateNodeData, "predicate">;

const STATUS_STYLES: Record<
  NodeStatus,
  { bg: string; border: string; text: string; fieldColor: string }
> = {
  idle: {
    bg: "#FFFFFF",
    border: "#E5E7EB",
    text: "#6B7280",
    fieldColor: "#9CA3AF",
  },
  active: {
    bg: "#FFFBEB",
    border: "#FCD34D",
    text: "#92400E",
    fieldColor: "#B45309",
  },
  done: {
    bg: "#F0FDF4",
    border: "#86EFAC",
    text: "#166534",
    fieldColor: "#15803D",
  },
};

export function PredicateNode({ data }: NodeProps<PredicateNodeType>) {
  const s = STATUS_STYLES[data.status];
  return (
    <div
      style={{
        width: 190,
        height: 56,
        background: s.bg,
        border: `2px solid ${s.border}`,
        borderRadius: 8,
        display: "flex",
        flexDirection: "column",
        justifyContent: "center",
        alignItems: "center",
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
        transition: "none",
        boxSizing: "border-box",
        padding: "4px 10px",
        position: "relative",
      }}
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{ opacity: 0, pointerEvents: "none" }}
      />
      <div
        style={{
          fontSize: 10,
          color: s.fieldColor,
          fontWeight: 500,
          letterSpacing: "0.03em",
          lineHeight: 1.2,
        }}
      >
        {data.field}
      </div>
      <div
        style={{
          fontSize: 13,
          fontWeight: 700,
          color: s.text,
          fontFamily: "monospace",
          lineHeight: 1.3,
        }}
      >
        {data.op} {data.value}
      </div>
      <div
        style={{
          position: "absolute",
          bottom: -10,
          fontSize: 9,
          color: "#9CA3AF",
          background: "#F9FAFB",
          border: "1px solid #E5E7EB",
          borderRadius: 999,
          padding: "1px 6px",
          lineHeight: 1.4,
          letterSpacing: "0.04em",
        }}
      >
        {data.layer}
      </div>
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ opacity: 0, pointerEvents: "none" }}
      />
    </div>
  );
}
