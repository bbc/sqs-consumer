/**
 * Delay function
 * @param {number} ms time in milliseconds
 * @returns {Promise<void>}
 */
export function delay(ms) {
  return new Promise((res) => setTimeout(res, ms));
}
