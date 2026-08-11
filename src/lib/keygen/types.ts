export interface KeyGenerator {
  createKey(keyLength: number): string;
}
