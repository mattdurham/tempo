import React, { useMemo } from "react";
import { ReactFlow, Background } from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import type { Node, Edge } from "@xyflow/react";
import { PredicateNode } from "./PredicateNode";
import { OperatorNode } from "./OperatorNode";
import {
  initialNodes,
  initialEdges,
  type QueryNode,
  type NodeStatus,
  type PredicateNodeData,
  type OperatorNodeData,
} from "../data/query";
import { STAGES } from "../data/pipeline";

type Props = {
  activePredicates: string[];
  stageIndex: number;
};

// Leaf predicate IDs for operator status computation
const OPERATOR_CHILDREN: Record<string, string[]> = {
  "root-and": ["svc", "dur-500", "sla", "dur-100"],
  "outer-or": ["dur-500", "sla", "dur-100"],
  "inner-and": ["sla", "dur-100"],
};

function getAllPastActivePredicates(stageIndex: number): Set<string> {
  const seen = new Set<string>();
  for (let i = 0; i <= stageIndex; i++) {
    for (const p of STAGES[i].activePredicates) {
      seen.add(p);
    }
  }
  return seen;
}

function computeNodeStatus(
  nodeId: string,
  isOperator: boolean,
  activePredicates: string[],
  stageIndex: number
): NodeStatus {
  const activeSet = new Set(activePredicates);
  const pastActive = getAllPastActivePredicates(stageIndex);

  if (isOperator) {
    const children = OPERATOR_CHILDREN[nodeId] ?? [];
    if (activeSet.has(nodeId)) return "active";
    const anyChildActive = children.some((c) => activeSet.has(c));
    if (anyChildActive) return "active";
    const allChildrenDone = children.every((c) => pastActive.has(c));
    if (allChildrenDone && children.length > 0) return "done";
    return "idle";
  }

  // leaf predicate
  if (activeSet.has(nodeId)) return "active";
  if (pastActive.has(nodeId)) return "done";
  return "idle";
}

function getEdgeStyle(
  sourceStatus: NodeStatus
): React.CSSProperties {
  if (sourceStatus === "active") {
    return { stroke: "#FCD34D", strokeWidth: 2 };
  }
  if (sourceStatus === "done") {
    return { stroke: "#86EFAC", strokeWidth: 1.5 };
  }
  return { stroke: "#E5E7EB", strokeWidth: 1 };
}

const nodeTypes = {
  predicate: PredicateNode,
  operator: OperatorNode,
};

export function PredicateTree({ activePredicates, stageIndex }: Props) {
  const nodes: QueryNode[] = useMemo(() => {
    return initialNodes.map((node) => {
      const isOperator = node.type === "operator";
      const status = computeNodeStatus(
        node.id,
        isOperator,
        activePredicates,
        stageIndex
      );
      if (isOperator) {
        return {
          ...node,
          data: {
            ...(node.data as OperatorNodeData),
            status,
          },
        } as Node<OperatorNodeData, "operator">;
      }
      return {
        ...node,
        data: {
          ...(node.data as PredicateNodeData),
          status,
        },
      } as Node<PredicateNodeData, "predicate">;
    });
  }, [activePredicates, stageIndex]);

  const statusMap = useMemo(() => {
    const m: Record<string, NodeStatus> = {};
    for (const n of nodes) {
      const isOperator = n.type === "operator";
      m[n.id] = computeNodeStatus(
        n.id,
        isOperator,
        activePredicates,
        stageIndex
      );
    }
    return m;
  }, [nodes, activePredicates, stageIndex]);

  const edges: Edge[] = useMemo(() => {
    return initialEdges.map((edge) => {
      const srcStatus = statusMap[edge.source] ?? "idle";
      const style = getEdgeStyle(srcStatus);
      return {
        ...edge,
        style,
      };
    });
  }, [statusMap]);

  return (
    <div style={{ width: "100%", height: "100%", background: "transparent" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        panOnDrag={false}
        zoomOnScroll={false}
        zoomOnPinch={false}
        zoomOnDoubleClick={false}
        preventScrolling={false}
        fitView
        fitViewOptions={{ padding: 0.12 }}
        proOptions={{ hideAttribution: true }}
        style={{ background: "transparent" }}
      >
        <Background color="transparent" />
      </ReactFlow>
    </div>
  );
}
