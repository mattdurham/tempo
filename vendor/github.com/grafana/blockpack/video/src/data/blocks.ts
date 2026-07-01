export type Block = {
  id: string;
  file: string;
  services: string[];
  durMin: number;
  durMax: number;
  sla: string[];
  spans: number;
  matchingSpans: number;
};

export const BLOCKS: Block[] = [
  {
    id: "blk-001",
    file: "f1",
    services: ["auth", "web"],
    durMin: 50,
    durMax: 600,
    sla: ["high", "low"],
    spans: 1240,
    matchingSpans: 3,
  },
  {
    id: "blk-002",
    file: "f1",
    services: ["web"],
    durMin: 10,
    durMax: 200,
    sla: ["low"],
    spans: 980,
    matchingSpans: 0,
  },
  {
    id: "blk-003",
    file: "f2",
    services: ["auth"],
    durMin: 200,
    durMax: 2000,
    sla: ["high"],
    spans: 870,
    matchingSpans: 7,
  },
  {
    id: "blk-004",
    file: "f2",
    services: ["auth", "cache"],
    durMin: 5,
    durMax: 80,
    sla: ["low"],
    spans: 2100,
    matchingSpans: 0,
  },
  {
    id: "blk-005",
    file: "f3",
    services: ["api", "web"],
    durMin: 100,
    durMax: 500,
    sla: ["medium"],
    spans: 1560,
    matchingSpans: 0,
  },
  {
    id: "blk-006",
    file: "f3",
    services: ["db"],
    durMin: 5,
    durMax: 50,
    sla: ["low"],
    spans: 730,
    matchingSpans: 0,
  },
  {
    id: "blk-007",
    file: "f4",
    services: ["auth"],
    durMin: 300,
    durMax: 1500,
    sla: ["high"],
    spans: 1100,
    matchingSpans: 5,
  },
  {
    id: "blk-008",
    file: "f4",
    services: ["auth"],
    durMin: 50,
    durMax: 450,
    sla: ["medium"],
    spans: 890,
    matchingSpans: 0,
  },
];
