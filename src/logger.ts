import createDebug from "debug";
const debug = createDebug("sqs-consumer");

export const logger = {
  debug,
  warn: (message: string) => {
    console.log(message);
  },
};
