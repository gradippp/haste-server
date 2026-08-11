"use client";

import {
  faEdit,
  faFile,
  faShare,
  faTrash,
  faEllipsisV,
} from "@fortawesome/free-solid-svg-icons";
import * as Tooltip from "@radix-ui/react-tooltip";
import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Swal from "sweetalert2";
import FooterButton from "./FooterButton";
import { LANGUAGE_OPTIONS, SHORTCUTS } from "@/utils/editor-commons";
import { useShortcuts } from "@/hooks/useShortcuts";
import { apiFetch, ApiError } from "@/lib/api";
import { forgetRevocationKey, getRevocationKey } from "@/lib/revocation";

interface ViewerFooterProps {
  id: string;
  language: string;
  title: string;
}

export default function ViewerFooter({
  id,
  language,
  title,
}: ViewerFooterProps) {
  const router = useRouter();
  const [showMenu, setShowMenu] = useState(false);
  // Only the creator holds this paste's revocation key, so the delete action
  // is shown only to them. Read after mount so server and client markup match.
  const [canDelete, setCanDelete] = useState(false);

  useEffect(() => {
    setCanDelete(Boolean(getRevocationKey(id)));
  }, [id]);

  const handleEdit = () => {
    router.push(`/?edit=${id}`);
  };

  const handleRaw = () => {
    if (typeof window !== "undefined") {
      window.location.href = `${window.location.pathname}/raw`;
    }
  };

  const handleDelete = async () => {
    const revocationKey = getRevocationKey(id);
    if (!revocationKey) return;

    const result = await Swal.fire({
      title: "Delete this paste?",
      text: "This cannot be undone.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonColor: "#dc2626",
      cancelButtonColor: "#aaa",
      confirmButtonText: "Yes, delete it",
      reverseButtons: true,
    });

    if (!result.isConfirmed) return;

    try {
      await apiFetch(
        `/api/pastes/${id}?revocation_key=${encodeURIComponent(revocationKey)}`,
        { method: "DELETE" }
      );
      forgetRevocationKey(id);
      router.push("/");
    } catch (error) {
      Swal.fire({
        icon: "error",
        title: "Error",
        text:
          error instanceof ApiError
            ? error.message
            : "Could not delete this paste.",
      });
    }
  };

  const handleShare = () => {
    if (typeof navigator !== "undefined" && navigator.share) {
      navigator
        .share({
          title: title || "haste paste",
          url: window.location.href,
        })
        .catch(() => {
          // User cancelled the share sheet — nothing to do.
        });
      return;
    }

    navigator.clipboard
      .writeText(window.location.href)
      .then(() => {
        Swal.fire({
          toast: true,
          position: "top-end",
          icon: "success",
          title: "Link copied to clipboard",
          showConfirmButton: false,
          timer: 2000,
          timerProgressBar: true,
        });
      })
      .catch(() => {
        Swal.fire({
          icon: "error",
          title: "Error",
          text: "Could not copy the link to the clipboard.",
        });
      });
  };

  useShortcuts({
    EDIT: { keys: SHORTCUTS.EDIT, handler: handleEdit },
    RAW: { keys: SHORTCUTS.RAW, handler: handleRaw },
    SHARE: { keys: SHORTCUTS.SHARE, handler: handleShare },
  });

  const languageLabel =
    LANGUAGE_OPTIONS.find((opt) => opt.value === language)?.label ||
    language ||
    "Auto Detect";

  return (
    <footer className="fixed bottom-0 right-0 h-12 w-full bg-gray-900 px-4 py-2 flex justify-between items-center gap-2 shadow z-20">
      <div className="flex items-center gap-2">
        <span className="bg-gray-800 text-gray-200 rounded px-2 py-1 text-sm">
          {languageLabel}
        </span>

        <input
          id="paste-title"
          type="text"
          placeholder="Paste Title"
          value={title}
          disabled
          className="bg-gray-800 text-gray-200 rounded px-2 py-1 text-sm focus:outline-none placeholder-gray-400 hidden sm:block cursor-not-allowed"
          readOnly
        />
      </div>
      <Tooltip.Provider delayDuration={300}>
        <div className="flex gap-4 items-center relative">
          <div className="hidden sm:flex gap-4">
            <FooterButton
              id="edit"
              shortcut={SHORTCUTS.EDIT}
              onClick={handleEdit}
              disabled={false}
              icon={faEdit}
              label="Edit"
            />
            <FooterButton
              id="raw"
              shortcut={SHORTCUTS.RAW}
              onClick={handleRaw}
              disabled={false}
              icon={faFile}
              label="View raw"
            />
            <FooterButton
              id="share"
              shortcut={SHORTCUTS.SHARE}
              onClick={handleShare}
              disabled={false}
              icon={faShare}
              label="Share"
            />
            {canDelete && (
              <FooterButton
                id="delete"
                shortcut={[]}
                onClick={handleDelete}
                disabled={false}
                icon={faTrash}
                label="Delete"
              />
            )}
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
                <input
                  id="paste-title-mobile"
                  type="text"
                  placeholder="Paste Title"
                  value={title}
                  disabled
                  readOnly
                  className="bg-gray-800 text-gray-200 rounded px-2 py-1 text-sm focus:outline-none placeholder-gray-400 cursor-not-allowed"
                />
                <hr />
                <FooterButton
                  id="edit-mobile"
                  shortcut={SHORTCUTS.EDIT}
                  onClick={handleEdit}
                  disabled={false}
                  icon={faEdit}
                  label="Edit"
                />
                <FooterButton
                  id="raw-mobile"
                  shortcut={SHORTCUTS.RAW}
                  onClick={handleRaw}
                  disabled={false}
                  icon={faFile}
                  label="View raw"
                />
                <FooterButton
                  id="share-mobile"
                  shortcut={SHORTCUTS.SHARE}
                  onClick={handleShare}
                  disabled={false}
                  icon={faShare}
                  label="Share"
                />
                {canDelete && (
                  <FooterButton
                    id="delete-mobile"
                    shortcut={[]}
                    onClick={handleDelete}
                    disabled={false}
                    icon={faTrash}
                    label="Delete"
                  />
                )}
              </div>
            )}
          </div>
        </div>
      </Tooltip.Provider>
    </footer>
  );
}
