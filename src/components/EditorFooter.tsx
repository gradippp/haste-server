"use client";

import { faFloppyDisk, faPlus, faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import * as Tooltip from "@radix-ui/react-tooltip";
import { useState } from "react";
import { useRouter } from "next/navigation";
import FooterButton from "./FooterButton";
import { LANGUAGE_OPTIONS, SHORTCUTS } from "@/utils/editor-commons";
import { useShortcuts } from "@/hooks/useShortcuts";

interface EditorFooterProps {
  language: string;
  setLanguage: (lang: string) => void;
  title: string;
  setTitle: (title: string) => void;
  setIsAutoDetect: (val: boolean) => void;
  handleSave: () => void;
  saveDisabled?: boolean;
  isEditSession?: boolean;
}

export default function EditorFooter({
  language,
  setLanguage,
  title,
  setTitle,
  setIsAutoDetect,
  handleSave,
  saveDisabled = false,
  isEditSession = false,
}: EditorFooterProps) {
  const router = useRouter();
  const [showMenu, setShowMenu] = useState(false);

  const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = e.target.value;
    setLanguage(value);
    setIsAutoDetect(value === "");
  };

  const onChangeTitle = (e: React.ChangeEvent<HTMLInputElement>) => {
    setTitle(e.target.value);
  };

  const handleNew = () => {
    router.push("/");
  };

  useShortcuts({
    SAVE: { keys: SHORTCUTS.SAVE, handler: handleSave },
    NEW: { keys: SHORTCUTS.NEW, handler: handleNew },
  });

  return (
    <footer className="fixed bottom-0 right-0 h-12 w-full bg-gray-900 px-4 py-2 flex justify-between items-center gap-2 shadow z-20">
      <div className="flex items-center gap-2">
        <label htmlFor="language-select" className="sr-only">
          Language
        </label>
        <select
          id="language-select"
          name="language"
          className="bg-gray-800 text-gray-200 rounded px-2 py-1 text-sm focus:outline-none"
          value={language}
          onChange={handleChange}
        >
          {LANGUAGE_OPTIONS.map(({ label, value }) => (
            <option key={value} value={value}>
              {label}
            </option>
          ))}
        </select>

        <label htmlFor="paste-title" className="sr-only">
          Paste title
        </label>
        <input
          id="paste-title"
          type="text"
          placeholder="Paste Title"
          value={title}
          className="bg-gray-800 text-gray-200 rounded px-2 py-1 text-sm focus:outline-none placeholder-gray-400 hidden sm:block"
          onChange={onChangeTitle}
        />
      </div>
      <Tooltip.Provider delayDuration={300}>
        <div className="flex gap-4 items-center relative">
          <div className="hidden sm:flex gap-4">
            <FooterButton
              id="new"
              shortcut={SHORTCUTS.NEW}
              onClick={handleNew}
              disabled={false}
              icon={faPlus}
              label="New paste"
            />
            <FooterButton
              id="save"
              shortcut={SHORTCUTS.SAVE}
              onClick={handleSave}
              disabled={saveDisabled}
              icon={faFloppyDisk}
              label="Save"
            />
          </div>

          <div className="sm:hidden relative">
            <FooterButton
              id="more"
              shortcut={[]}
              onClick={() => setShowMenu(!showMenu)}
              icon={faEllipsisV}
              disabled={false}
              label="More actions"
            />
            {showMenu && (
              <div className="absolute bottom-12 right-0 bg-gray-800 text-white rounded shadow-md p-2 flex flex-col z-30 min-w-[150px]">
                <label htmlFor="paste-title-mobile" className="sr-only">
                  Paste title
                </label>
                <input
                  id="paste-title-mobile"
                  type="text"
                  placeholder="Paste Title"
                  value={title}
                  className="bg-gray-800 text-gray-200 rounded px-2 py-1 text-sm focus:outline-none placeholder-gray-400"
                  onChange={onChangeTitle}
                />
                <hr />
                <FooterButton
                  id="new-mobile"
                  shortcut={SHORTCUTS.NEW}
                  onClick={handleNew}
                  disabled={false}
                  icon={faPlus}
                  label="New paste"
                />
                <FooterButton
                  id="save-mobile"
                  shortcut={SHORTCUTS.SAVE}
                  onClick={handleSave}
                  disabled={saveDisabled}
                  icon={faFloppyDisk}
                  label="Save"
                />
              </div>
            )}
          </div>
        </div>
      </Tooltip.Provider>
      {isEditSession && (
        <span className="hidden md:inline text-xs text-gray-500 absolute left-1/2 -translate-x-1/2">
          Saving creates a new paste — the original is not overwritten.
        </span>
      )}
    </footer>
  );
}
