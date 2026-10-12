/**
 * Display segment for a reference THROUGH a relationship: `[A/<rel name>/<target display>]`.
 *
 * Sigma stores an unnamed passthrough column under its canonical display name (minor words after the first are
 * lowercased: "Day Of Month" -> "Day of Month"). Same-element refs fold case, but a ref through a relationship
 * must use the canonical casing. An explicit `name` is used verbatim. Word splitting is the engine's, untouched.
 * Must stay browser-safe.
 */
import { SIGMA_LOWERCASE_WORDS } from '../../src/sigma-ids.js';

export function relDisplaySegment(display: string, explicitName: boolean): string {
  if (explicitName) return display;
  return display
    .split(' ')
    .map((w, i) => (i > 0 && SIGMA_LOWERCASE_WORDS.has(w.toLowerCase()) ? w.toLowerCase() : w))
    .join(' ');
}
