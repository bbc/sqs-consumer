export class SQSError extends Error {
  code: string;
  statusCode: number;
  region: string;
  hostname: string;
  time: Date;
  retryable: boolean;

  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}
