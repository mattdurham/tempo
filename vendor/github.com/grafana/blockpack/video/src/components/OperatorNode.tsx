import React from "react";
import { Handle, Position } from "@xyflow/react";
import type { NodeProps, Node } from "@xyflow/react";
import type { OperatorNodeData, NodeStatus } from "../data/query";

export type OperatorNodeType = Node<OperatorNodeData, "operator">;

const STATUS_STYLES: Record<
  NodeStatus,
  { bg: string; border: string; text: string }
> = {
  idle: { bg: "#F3F4F6", border: "#D1D5DB", text: "#6B7280" },
  active: { bg: "#EFF6FF", border: "#93C5FD", text: "#1D4ED8" },
  done: { bg: "#F0FDF4", border: "#86EFAC", text: "#15803D" },
};

export function OperatorNode({ data }: NodeProps<OperatorNodeType>) {
  const s = STATUS_STYLES[data.status];
  return (
    <div
      style={{
        width: 72,
        height: 32,
        background: s.bg,
        border: `2px solid ${s.border}`,
        borderRadius: 999,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        fontFamily: "system-ui, -apple-system, 'Segoe UI', sans-serif",
        fontSize: 13,
        fontWeight: 700,
        color: s.text,
        letterSpacing: "0.05em",
        transition: "none",
        boxSizing: "border-box",
      }}
    >
      <Handle
        type="target"
        position={Position.Top}
        style={{ opacity: 0, pointerEvents: "none" }}
      />
      {data.operator}
      <Handle
        type="source"
        position={Position.Bottom}
        style={{ opacity: 0, pointerEvents: "none" }}
      />
    </div>
  );
}
