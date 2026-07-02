import type { Node, Edge } from "@xyflow/react";

export type NodeStatus = "idle" | "active" | "done";

export type PredicateNodeData = {
  field: string;
  op: string;
  value: string;
  layer: string;
  status: NodeStatus;
};

export type OperatorNodeData = {
  operator: "AND" | "OR";
  status: NodeStatus;
};

export type QueryNode =
  | Node<PredicateNodeData, "predicate">
  | Node<OperatorNodeData, "operator">;

export const QUERY_STRING =
  '{ span.service.name = "auth" && (duration > 500ms || (span.sla = "high" && duration > 100ms)) }';

// Node dimensions — must match the rendered sizes in the custom node components
// so React Flow can route edges correctly without relying on DOM measurement.
export const OP_W = 72;
export const OP_H = 32;
export const PRED_W = 190;
export const PRED_H = 56;

export const initialNodes: QueryNode[] = [
  {
    id: "root-and",
    type: "operator",
    position: { x: 0, y: 0 },
    width: OP_W,
    height: OP_H,
    data: { operator: "AND", status: "idle" },
  },
  {
    id: "svc",
    type: "predicate",
    position: { x: -260, y: 130 },
    width: PRED_W,
    height: PRED_H,
    data: {
      field: "span.service.name",
      op: "=",
      value: '"auth"',
      layer: "intrinsic",
      status: "idle",
    },
  },
  {
    id: "outer-or",
    type: "operator",
    position: { x: 220, y: 130 },
    width: OP_W,
    height: OP_H,
    data: { operator: "OR", status: "idle" },
  },
  {
    id: "dur-500",
    type: "predicate",
    position: { x: 60, y: 280 },
    width: PRED_W,
    height: PRED_H,
    data: {
      field: "duration",
      op: ">",
      value: "500ms",
      layer: "range",
      status: "idle",
    },
  },
  {
    id: "inner-and",
    type: "operator",
    position: { x: 370, y: 280 },
    width: OP_W,
    height: OP_H,
    data: { operator: "AND", status: "idle" },
  },
  {
    id: "sla",
    type: "predicate",
    position: { x: 240, y: 430 },
    width: PRED_W,
    height: PRED_H,
    data: {
      field: "span.sla",
      op: "=",
      value: '"high"',
      layer: "bloom",
      status: "idle",
    },
  },
  {
    id: "dur-100",
    type: "predicate",
    position: { x: 480, y: 430 },
    width: PRED_W,
    height: PRED_H,
    data: {
      field: "duration",
      op: ">",
      value: "100ms",
      layer: "range",
      status: "idle",
    },
  },
];

export const initialEdges: Edge[] = [
  { id: "e-root-svc", source: "root-and", target: "svc" },
  { id: "e-root-or", source: "root-and", target: "outer-or" },
  { id: "e-or-dur500", source: "outer-or", target: "dur-500" },
  { id: "e-or-iand", source: "outer-or", target: "inner-and" },
  { id: "e-iand-sla", source: "inner-and", target: "sla" },
  { id: "e-iand-dur100", source: "inner-and", target: "dur-100" },
];
