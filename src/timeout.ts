import { TimeoutResponse } from './types';
import { TimeoutError } from './errors';

/**
 * Create a timeout.
 * @param duration The duration of the timeout.
 */
function createTimeout(duration: number): TimeoutResponse[] {
  let timeout;
  const pending = new Promise((_, reject) => {
    timeout = setTimeout((): void => {
      reject(new TimeoutError());
    }, duration);
  });
  return [timeout, pending];
}

export { createTimeout };
