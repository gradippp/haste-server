"use client";

import { useEffect, useRef } from "react";
import "@catppuccin/highlightjs/css/catppuccin-mocha.css";
import "@fontsource/jetbrains-mono/400.css";
import ViewerFooter from "@/components/ViewerFooter";
import LineNumbers from "@/components/LineNumbers";

interface PasteViewerProps {
  id: string;
  title: string;
  language: string;
  lineCount: number;
  highlightedHtml: string;
}

export default function PasteViewer({
  id,
  title,
  language,
  lineCount,
  highlightedHtml,
}: PasteViewerProps) {
  const linenosRef = useRef<HTMLDivElement>(null);
  const codeElementRef = useRef<HTMLElement>(null);

  useEffect(() => {
    const linenos = linenosRef.current;
    const code = codeElementRef.current;

    if (!linenos || !code) return;

    const handleScroll = () => {
      linenos.scrollTop = code.scrollTop;
    };

    code.addEventListener("scroll", handleScroll);
    return () => {
      code.removeEventListener("scroll", handleScroll);
    };
  }, []);

  return (
    <>
      <div className="flex flex-auto w-full">
        <LineNumbers lineCount={lineCount} scrollRef={linenosRef} variant="viewer" />

        <pre className="flex flex-auto h-full w-full text-xs pb-14">
          <code
            ref={codeElementRef}
            className="w-full overflow-auto"
            style={{
              display: "block",
              height: "100%",
              fontFamily: "JetBrains Mono",
            }}
            dangerouslySetInnerHTML={{ __html: highlightedHtml }}
          />
        </pre>
      </div>
      <ViewerFooter id={id} language={language} title={title} />
    </>
  );
}
