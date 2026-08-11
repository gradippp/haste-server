'use client';

import * as Tooltip from '@radix-ui/react-tooltip';
import { IconDefinition } from '@fortawesome/fontawesome-svg-core';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

interface FooterButtonProps {
  id: string;
  shortcut: string[];
  onClick: () => void;
  disabled: boolean;
  icon: IconDefinition;
  label?: string;
}

export default function FooterButton({
  id,
  shortcut,
  onClick,
  disabled,
  icon,
  label,
}: FooterButtonProps) {
  return (
    <Tooltip.Root>
      <Tooltip.Trigger asChild>
        <button
          id={`action-${id}`}
          aria-label={label}
          className={`p-2 rounded hover:bg-gray-800 text-gray-300 ${
            disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
          }`}
          onClick={onClick}
          disabled={disabled}
        >
          <FontAwesomeIcon icon={icon} />
        </button>
      </Tooltip.Trigger>
      <Tooltip.Portal>
        <Tooltip.Content
          side="top"
          sideOffset={6}
          className="bg-gray-900 text-white text-xs px-2 py-1 rounded shadow-md z-50"
        >
          {shortcut.join(' + ')}
          <Tooltip.Arrow className="fill-gray-900" />
        </Tooltip.Content>
      </Tooltip.Portal>
    </Tooltip.Root>
  );
}
