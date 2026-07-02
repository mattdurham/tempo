export type PipelineStage = {
  id: string;
  label: string;
  description: string;
  activePredicates: string[];
  prunedBlocks: string[];
  pruneReason: string;
};

export const INTRO_FRAMES = 30;
export const STAGE_FRAMES = 100; // 3.3s per stage

export const STAGES: PipelineStage[] = [
  {
    id: "parse",
    label: "Query Parse",
    description: "Decompose into AND/OR predicate tree",
    activePredicates: [],
    prunedBlocks: [],
    pruneReason: "",
  },
  {
    id: "file-bloom",
    label: "File-Level Metadata",
    description:
      'Bloom filter on file metadata — does this file contain service "auth"?',
    activePredicates: ["svc"],
    prunedBlocks: ["blk-005", "blk-006"],
    pruneReason: 'file has no "auth" service',
  },
  {
    id: "intrinsic",
    label: "Intrinsic Filter",
    description:
      'Block-level service name check — prune blocks with no "auth" spans',
    activePredicates: ["svc"],
    prunedBlocks: ["blk-002"],
    pruneReason: 'block has no "auth" spans',
  },
  {
    id: "kll",
    label: "KLL Range Index",
    description:
      "Duration range check — block max duration must exceed lowest threshold",
    activePredicates: ["dur-500", "dur-100"],
    prunedBlocks: ["blk-004"],
    pruneReason: "max duration 80ms < 100ms",
  },
  {
    id: "bloom",
    label: "Block Bloom Filter",
    description:
      'Per-column bloom filter — does this block contain sla = "high"?',
    activePredicates: ["sla"],
    prunedBlocks: ["blk-008"],
    pruneReason: 'no "high" sla value',
  },
  {
    id: "cms",
    label: "CMS Zero-Estimate",
    description:
      "Count-Min Sketch: estimate of 0 means value is definitely absent — safe prune",
    activePredicates: ["svc", "sla"],
    prunedBlocks: [],
    pruneReason: "",
  },
  {
    id: "span-eval",
    label: "Span Evaluation",
    description:
      "ONE I/O per block — evaluate full predicate against each span",
    activePredicates: [
      "root-and",
      "svc",
      "outer-or",
      "dur-500",
      "inner-and",
      "sla",
      "dur-100",
    ],
    prunedBlocks: [],
    pruneReason: "",
  },
  {
    id: "results",
    label: "Results",
    description: "15 matching spans across 3 blocks",
    activePredicates: [],
    prunedBlocks: [],
    pruneReason: "",
  },
];

export const TOTAL_FRAMES = INTRO_FRAMES + STAGES.length * STAGE_FRAMES;

export function getStageStartFrame(index: number): number {
  return INTRO_FRAMES + index * STAGE_FRAMES;
}

export function getActiveStageIndex(frame: number): number {
  if (frame < INTRO_FRAMES) return -1;
  const idx = Math.floor((frame - INTRO_FRAMES) / STAGE_FRAMES);
  return Math.min(idx, STAGES.length - 1);
}

export function getPrunedBlockIds(upToStageIndex: number): Set<string> {
  const pruned = new Set<string>();
  for (let i = 0; i <= upToStageIndex; i++) {
    for (const id of STAGES[i].prunedBlocks) {
      pruned.add(id);
    }
  }
  return pruned;
}
