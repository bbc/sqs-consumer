export declare class SQSError extends Error {
    code: string;
    statusCode: number;
    region: string;
    hostname: string;
    time: Date;
    retryable: boolean;
    constructor(message: string);
}
