"use client";

import { useEffect, useState } from "react";
import Swal from "sweetalert2";
import { getPaste } from "@/lib/api";
import type { PasteContent } from "@/types/paste";

export function usePaste(id: string | null) {
  const [paste, setPaste] = useState<PasteContent | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id) {
      setPaste(null);
      setIsLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;

    const fetchPaste = async () => {
      setIsLoading(true);
      setError(null);
      try {
        const data = await getPaste(id);
        if (cancelled) return;
        setPaste(data);
      } catch (err) {
        if (cancelled) return;
        const message =
          err instanceof Error ? err.message : "Failed to load paste.";
        setError(message);
        Swal.fire({
          icon: "error",
          title: "Error",
          text: message,
        });
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    };

    fetchPaste();

    return () => {
      cancelled = true;
    };
  }, [id]);

  return { paste, isLoading, error };
}
