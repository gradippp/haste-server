"use client";

import {
  useState,
  useRef,
  useEffect,
  useMemo,
  useCallback,
  Suspense,
} from "react";
import { useRouter, useSearchParams } from "next/navigation";
import EditorFooter from "@/components/EditorFooter";
import LineNumbers from "@/components/LineNumbers";
import { detectLanguage, highlight } from "@/lib/highlight";
import Swal from "sweetalert2";
import { apiFetch, ApiError } from "@/lib/api";
import {
  forgetRevocationKey,
  getRevocationKey,
  rememberRevocationKey,
} from "@/lib/revocation";
import { usePaste } from "@/hooks/usePaste";
import "@catppuccin/highlightjs/css/catppuccin-mocha.css";
import "@fontsource/jetbrains-mono/400.css";

// Above this size, skip the highlighted overlay and fall back to a plain
// textarea so huge pastes stay responsive.
const HIGHLIGHT_LINE_THRESHOLD = 3000;
const HIGHLIGHT_BYTE_THRESHOLD = 200 * 1024; // 200 KB

// Auto-detection only needs a sample of the content, not the whole document.
const DETECT_SAMPLE_SIZE = 2000;
const DETECT_DEBOUNCE_MS = 300;

// Hard cap on paste size — saving is disabled above this. Advisory only; the
// server enforces its own `maxLength`.
const MAX_CONTENT_BYTES = 1024 * 1024; // 1 MB

export default function HomePage() {
  return (
    <Suspense fallback={null}>
      <HomePageInner />
    </Suspense>
  );
}

function HomePageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const editId = searchParams.get("edit");

  const [content, setContent] = useState(``);
  const [language, setLanguage] = useState(""); // '' means auto-detect or none selected
  const [title, setTitle] = useState("");
  const [isAutoDetect, setIsAutoDetect] = useState(true); // default to auto-detect
  const linenosRef = useRef<HTMLDivElement>(null);
  const preRef = useRef<HTMLPreElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const detectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const { paste } = usePaste(editId);

  const lineCount = useMemo(() => content.split("\n").length, [content]);
  const contentBytes = useMemo(() => new Blob([content]).size, [content]);

  const isOverSizeLimit = contentBytes > MAX_CONTENT_BYTES;

  const skipHighlight =
    lineCount > HIGHLIGHT_LINE_THRESHOLD ||
    contentBytes > HIGHLIGHT_BYTE_THRESHOLD;

  // Sync scroll between textarea, the highlighted <pre>, and the line-number
  // gutter. The textarea remains the source of truth for scroll position.
  const handleScroll = useCallback(() => {
    const textarea = textareaRef.current;
    if (!textarea) return;
    if (linenosRef.current) {
      linenosRef.current.scrollTop = textarea.scrollTop;
    }
    if (preRef.current) {
      preRef.current.scrollTop = textarea.scrollTop;
      preRef.current.scrollLeft = textarea.scrollLeft;
    }
  }, []);

  const highlightedHtml = useMemo(() => {
    if (skipHighlight) return "";
    return highlight(content, language);
  }, [content, language, skipHighlight]);

  const handleSave = async () => {
    if (isOverSizeLimit) return;
    try {
      const formData = new FormData();
      formData.append("content", content);
      formData.append("language", language);
      formData.append("title", title);

      const data = await apiFetch<{ id: string; revocation_key?: string }>(
        "/api/pastes",
        {
          method: "POST",
          body: formData,
        }
      );

      if (data.id && data.revocation_key) {
        rememberRevocationKey(data.id, data.revocation_key);
      }

      // Edit-in-place: there is no update endpoint, so re-saving an edited
      // paste always creates a new one. If we hold the original's revocation
      // key, clean up the orphaned original with a best-effort,
      // fire-and-forget DELETE so re-saves don't accumulate duplicates.
      if (editId) {
        const revocationKey = getRevocationKey(editId);
        if (revocationKey) {
          apiFetch(
            `/api/pastes/${editId}?revocation_key=${encodeURIComponent(
              revocationKey
            )}`,
            { method: "DELETE" }
          ).catch(() => {
            // Best effort — ignore failures.
          });
          forgetRevocationKey(editId);
        }
      }

      router.push(`/${data.id}`);
    } catch (error) {
      Swal.fire({
        icon: "error",
        title: "Error",
        text:
          error instanceof ApiError
            ? error.message
            : "An error occurred while saving.",
      });
      console.error(error);
    }
  };

  // Auto-detect language on content change only if language is set to auto
  // (empty string). Debounced and restricted to a small sample so it isn't
  // redoing full-document work on every keystroke.
  useEffect(() => {
    if (!isAutoDetect) return;

    if (detectTimeoutRef.current) clearTimeout(detectTimeoutRef.current);
    detectTimeoutRef.current = setTimeout(() => {
      setLanguage(detectLanguage(content.slice(0, DETECT_SAMPLE_SIZE)));
    }, DETECT_DEBOUNCE_MS);

    return () => {
      if (detectTimeoutRef.current) clearTimeout(detectTimeoutRef.current);
    };
  }, [content, isAutoDetect]);

  // Handle edit sessions
  useEffect(() => {
    if (!paste) return;
    setContent(paste.content || "");
    if (paste.language) setLanguage(paste.language);
    if (paste.title) setTitle(paste.title);
    // Scroll to bottom once the existing paste has finished loading.
    requestAnimationFrame(() => {
      if (textareaRef.current && linenosRef.current) {
        textareaRef.current.scrollTop = textareaRef.current.scrollHeight;
        linenosRef.current.scrollTop = linenosRef.current.scrollHeight;
        if (preRef.current)
          preRef.current.scrollTop = preRef.current.scrollHeight;
      }
    });
  }, [paste]);

  const handleFile = useCallback((file: File) => {
    const reader = new FileReader();
    reader.onload = () => {
      const text = typeof reader.result === "string" ? reader.result : "";
      setContent(text);
      setTitle((prev) => (prev ? prev : file.name));
    };
    reader.readAsText(file);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent<HTMLDivElement>) => {
      e.preventDefault();
      const file = e.dataTransfer.files?.[0];
      if (file) handleFile(file);
    },
    [handleFile]
  );

  const charCount = content.length;

  return (
    <>
      <div
        className="flex flex-1 w-full overflow-hidden pb-4 flex-col"
        style={{ height: "calc(100vh - 48px)" }}
      >
        <div className="flex justify-between items-center px-2 pt-1 pb-1 text-xs text-gray-500">
          <span>
            {lineCount} lines &middot; {charCount.toLocaleString()} chars
          </span>
          {isOverSizeLimit && (
            <span className="text-red-400">Content exceeds the 1MB limit</span>
          )}
        </div>

        <div
          className="flex flex-1 w-full overflow-hidden"
          onDrop={handleDrop}
          onDragOver={(e) => e.preventDefault()}
        >
          <LineNumbers
            lineCount={lineCount}
            scrollRef={linenosRef}
            variant="editor"
          />

          <TextArea
            content={content}
            onChange={setContent}
            textareaRef={textareaRef}
            preRef={preRef}
            onScroll={handleScroll}
            highlightedHtml={highlightedHtml}
            skipHighlight={skipHighlight}
          />
        </div>
      </div>

      <EditorFooter
        language={language}
        setLanguage={setLanguage}
        setTitle={setTitle}
        setIsAutoDetect={setIsAutoDetect}
        title={title}
        handleSave={handleSave}
        saveDisabled={isOverSizeLimit}
        isEditSession={Boolean(editId)}
      />
    </>
  );
}

function TextArea({
  content,
  onChange,
  textareaRef,
  preRef,
  onScroll,
  highlightedHtml,
  skipHighlight,
}: {
  content: string;
  onChange: (val: string) => void;
  textareaRef: React.RefObject<HTMLTextAreaElement | null>;
  preRef: React.RefObject<HTMLPreElement | null>;
  onScroll: () => void;
  highlightedHtml: string;
  skipHighlight: boolean;
}) {
  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Tab") {
      e.preventDefault(); // Prevent focus change
      const textarea = e.currentTarget;
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const value = textarea.value;

      const newValue =
        value.substring(0, start) + "    " + value.substring(end); // 4 spaces
      onChange(newValue);

      // Move the cursor
      setTimeout(() => {
        textarea.selectionStart = textarea.selectionEnd = start + 4;
      }, 0);
    }
  };

  const sharedFontStyle: React.CSSProperties = {
    fontFamily: "JetBrains Mono",
    fontSize: "0.875rem", // text-sm
    lineHeight: "1.25rem",
    letterSpacing: "normal",
    whiteSpace: "pre",
    boxSizing: "border-box",
    padding: 0,
    margin: 0,
  };

  if (skipHighlight) {
    return (
      <textarea
        ref={textareaRef}
        className="flex-1 h-full w-full resize-none bg-transparent text-white text-sm outline-none border-none overflow-y-auto"
        value={content}
        wrap="off"
        placeholder="Write your code here..."
        onChange={(e) => onChange(e.target.value)}
        onScroll={onScroll}
        onKeyDown={handleKeyDown}
        spellCheck={false}
        style={sharedFontStyle}
      />
    );
  }

  return (
    <div className="relative flex-1 h-full w-full overflow-hidden">
      <pre
        ref={preRef}
        aria-hidden
        className="absolute inset-0 overflow-auto pointer-events-none"
        style={sharedFontStyle}
      >
        <code
          style={{ ...sharedFontStyle, display: "block" }}
          dangerouslySetInnerHTML={{ __html: highlightedHtml || "​" }}
        />
      </pre>
      <textarea
        ref={textareaRef}
        className="absolute inset-0 h-full w-full resize-none bg-transparent outline-none border-none overflow-auto"
        value={content}
        wrap="off"
        placeholder="Write your code here..."
        onChange={(e) => onChange(e.target.value)}
        onScroll={onScroll}
        onKeyDown={handleKeyDown}
        spellCheck={false}
        style={{
          ...sharedFontStyle,
          color: "transparent",
          caretColor: "white",
          background: "transparent",
        }}
      />
    </div>
  );
}
