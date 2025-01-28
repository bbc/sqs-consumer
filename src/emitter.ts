import { EventEmitter } from "node:events";

import { logger } from "./logger.js";
import { Events, QueueMetadata } from "./types.js";

export class TypedEventEmitter extends EventEmitter {
  protected queueUrl?: string;

  constructor(queueUrl?: string) {
    super();
    this.queueUrl = queueUrl;
  }

  /**
   * Trigger a listener on all emitted events
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  on<E extends keyof Events>(
    event: E,
    listener: (...args: [...Events[E], QueueMetadata]) => void,
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
    listener: (...args: [...Events[E], QueueMetadata]) => void,
  ): this {
    return super.once(event, listener);
  }

  /**
   * Emits an event with the provided arguments
   * @param event The name of the event to emit
   * @param args The arguments to pass to the event listeners
   * @returns {boolean} Returns true if the event had listeners, false otherwise
   */
  emit<E extends keyof Events>(event: E, ...args: Events[E]): boolean {
    const metadata: QueueMetadata = { queueUrl: this.queueUrl };
    logger.debug(event, ...args, metadata);
    return super.emit(event, ...args, metadata);
  }
}
