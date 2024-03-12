import { EventEmitter } from "node:events";

import { logger } from "./logger.js";
import { Events } from "./types.js";

export class TypedEventEmitter extends EventEmitter {
  /**
   * Trigger a listener on all emitted events
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  on<E extends keyof Events>(
    event: E,
    listener: (...args: Events[E]) => void,
  ): this {
    return super.on(event, listener);
  }
  /**
   * Trigger a listener only once for an emitted event
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  once<E extends keyof Events>(
    event: E,
    listener: (...args: Events[E]) => void,
  ): this {
    return super.once(event, listener);
  }
  /**
   * Emits an event with the provided arguments
   * @param event The name of the event to emit
   */
  emit<E extends keyof Events>(event: E, ...args: Events[E]): boolean {
    logger.debug(event, ...args);
    return super.emit(event, ...args);
  }
}
