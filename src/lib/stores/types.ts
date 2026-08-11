/**
 * Promise-based replacement for the old callback `DocumentStore` contract.
 *
 * `skipExpire` carries the same meaning as before: when true, a read must not
 * bump a document's expiry and a write must not set one. It's used for the
 * static documents seeded from `config.documents` and while probing for a
 * free key.
 */
export interface DocumentStore {
  /** Returns the stored string, or null when the key is absent/expired. */
  get(key: string, skipExpire?: boolean): Promise<string | null>;

  /** Returns true on success. */
  set(key: string, data: string, skipExpire?: boolean): Promise<boolean>;

  /**
   * Deletes a document. Optional: backends that can't support it are allowed
   * to omit it, and DELETE /api/pastes/:id then answers 501.
   */
  del?(key: string): Promise<boolean>;
}

export interface StoreFactoryOptions {
  [key: string]: unknown;
  expire?: number;
}
