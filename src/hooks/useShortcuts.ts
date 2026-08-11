"use client";

import { useEffect, useRef } from "react";

/**
 * Registers global keyboard shortcuts once on mount. Handlers are held in a
 * ref so callers can pass a fresh object/functions every render without
 * causing the underlying event listeners to be torn down and re-added.
 *
 * `shortcuts` maps a shortcut name (matching a key in `SHORTCUTS` from
 * `@/utils/editor-commons`) to the key combination (array of key names) and
 * the handler to invoke when it's pressed.
 */
export function useShortcuts(
  shortcuts: Record<string, { keys: string[]; handler: () => void }>
) {
  const shortcutsRef = useRef(shortcuts);
  shortcutsRef.current = shortcuts;

  useEffect(() => {
    const pressed = new Set<string>();
    const normalizeKey = (key: string) => key.toLowerCase();

    const handleKeyDown = (e: KeyboardEvent) => {
      pressed.add(normalizeKey(e.key));

      const matches = (keys: string[]) =>
        keys.every((code) => pressed.has(normalizeKey(code)));

      for (const { keys, handler } of Object.values(shortcutsRef.current)) {
        if (matches(keys)) {
          e.preventDefault();
          handler();
          break;
        }
      }
    };

    const handleKeyUp = (e: KeyboardEvent) => {
      pressed.delete(normalizeKey(e.key));
    };

    const handleBlur = () => {
      pressed.clear();
    };

    window.addEventListener("keydown", handleKeyDown);
    window.addEventListener("keyup", handleKeyUp);
    window.addEventListener("blur", handleBlur);

    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("keyup", handleKeyUp);
      window.removeEventListener("blur", handleBlur);
    };
  }, []);
}
