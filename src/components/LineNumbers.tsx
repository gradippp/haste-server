"use client";

import { memo, RefObject } from "react";

const MAX_RENDERED_LINES = 5000;

interface LineNumbersProps {
  lineCount: number;
  scrollRef?: RefObject<HTMLDivElement | null>;
  variant?: "editor" | "viewer";
}

/**
 * Shared line-number gutter used by both the editor and the paste viewer.
 * Purely presentational — the parent is responsible for syncing scroll
 * position onto `scrollRef.current.scrollTop` from whatever surface it's
 * paired with (a textarea or a <pre><code> block).
 *
 * For very large pastes, rendering one <div> per line gets expensive, so
 * above MAX_RENDERED_LINES we just show a capped set of line numbers plus
 * an indicator that numbering was truncated.
 */
function LineNumbers({ lineCount, scrollRef, variant = "editor" }: LineNumbersProps) {
  const capped = lineCount > MAX_RENDERED_LINES;
  const renderCount = capped ? MAX_RENDERED_LINES : lineCount;

  const className =
    variant === "editor"
      ? "top-5 left-0 pr-5 select-none text-sm overflow-auto h-full text-gray-500"
      : "select-none overflow-auto h-full min-w-fit text-gray-700 pb-14 pr-5 text-xs";

  return (
    <div
      ref={scrollRef}
      className={className}
      style={{
        minWidth: 32,
        scrollbarWidth: "none",
        pointerEvents: variant === "viewer" ? "none" : undefined,
        userSelect: "none",
      }}
    >
      {Array.from({ length: renderCount }, (_, i) => (
        <div key={i}>{i + 1}</div>
      ))}
      {capped && <div>&hellip;</div>}
    </div>
  );
}

export default memo(LineNumbers);
