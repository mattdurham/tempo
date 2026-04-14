export type WriteStage = {
  id: string;
  label: string;
  description: string;
  activeSection: string[];
};

export const WRITE_STAGES: WriteStage[] = [
  {
    id: "ingest",
    label: "Span Ingestion",
    description:
      "AddTracesData buffers lightweight pendingSpan records (88 bytes each, deferred materialization)",
    activeSection: ["pending-spans"],
  },
  {
    id: "minhash",
    label: "MinHash Clustering",
    description:
      "Sort spans by (service.name, MinHash signature, TraceID) — clusters similar spans into the same block",
    activeSection: ["pending-spans"],
  },
  {
    id: "batching",
    label: "Block Batching",
    description:
      "Group ~2000 spans per block; emit block boundaries; assign ULIDs",
    activeSection: ["block-payloads"],
  },
  {
    id: "encoding",
    label: "Column Encoding",
    description:
      "Select encoding per column (11 schemes: Dictionary, Delta Uint64, RLE Indexes, XOR Bytes, Prefix Bytes, Delta Dictionary + sparse variants); apply Zstd compression",
    activeSection: ["block-payloads"],
  },
  {
    id: "kll",
    label: "KLL Sketches",
    description:
      "Build Karnin-Lang-Liberty range sketch per numeric column — enables duration/timestamp range pruning",
    activeSection: ["kll"],
  },
  {
    id: "sketches",
    label: "CMS + HLL + TopK",
    description:
      "Count-Min Sketch (512B/block), HyperLogLog (4B/column), Top-20 values — power block scoring and CMS zero-estimate pruning",
    activeSection: ["cms-hll-topk"],
  },
  {
    id: "bloom",
    label: "Bloom Filters",
    description:
      "BinaryFuse8 per-column filter (~1B/entry, 0.4% FPR) — enables fast per-block value membership tests",
    activeSection: ["bloom"],
  },
  {
    id: "intrinsics",
    label: "Intrinsic Columns",
    description:
      "Accumulate 8 core span attributes (trace:id, span:id, span:name, duration, start, kind, service.name, log:timestamp) at file level",
    activeSection: ["intrinsics"],
  },
  {
    id: "indexes",
    label: "Indexes + FBLM",
    description:
      "Write trace block index, TS index, dedicated column indexes, file-level BinaryFuse8 bloom (FBLM) over service.name",
    activeSection: ["trace-index", "fblm", "header"],
  },
];

export const WRITE_INTRO_FRAMES = 30;
export const WRITE_STAGE_FRAMES = 100;
export const WRITE_TOTAL_FRAMES =
  WRITE_INTRO_FRAMES + WRITE_STAGES.length * WRITE_STAGE_FRAMES;

export function getWriteStageIndex(frame: number): number {
  if (frame < WRITE_INTRO_FRAMES) return -1;
  return Math.min(
    Math.floor((frame - WRITE_INTRO_FRAMES) / WRITE_STAGE_FRAMES),
    WRITE_STAGES.length - 1,
  );
}

export function getWriteStageStartFrame(index: number): number {
  return WRITE_INTRO_FRAMES + index * WRITE_STAGE_FRAMES;
}
