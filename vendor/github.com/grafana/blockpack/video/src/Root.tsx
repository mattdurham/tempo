import React from "react";
import { Composition } from "remotion";
import { Pipeline } from "./Pipeline";
import { TOTAL_FRAMES } from "./data/pipeline";
import { WritePath } from "./WritePath";
import { WRITE_TOTAL_FRAMES } from "./data/writepath";

const FPS = 30;

export const Root: React.FC = () => (
  <>
    <Composition
      id="Pipeline"
      component={Pipeline}
      durationInFrames={TOTAL_FRAMES}
      fps={FPS}
      width={1280}
      height={720}
    />
    <Composition
      id="WritePath"
      component={WritePath}
      durationInFrames={WRITE_TOTAL_FRAMES}
      fps={FPS}
      width={1280}
      height={720}
    />
  </>
);
