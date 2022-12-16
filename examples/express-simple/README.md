# SQS Consumer Example App - Express Simple

This is an example app to show how SQS Consumer can be used, while also allowing us to test that the library is working correctly between changes, and with new functionality.

Please use this app when contributing changes to test your work with known functionality.

## Run the app

To get started, you will need Docker installed and started on your machine, you can [find instructions on how to install Docker here](https://docs.docker.com/get-docker/).

Once installed, just enter the command `npm run start` and after a few moments, the example app should be available at `http://localhost:3026`.

If you'd prefer to run without Docker, you can run the command `npm run start:node` instead.

> **Note**
> If you do not use the Docker stack, you will need to start a local version of SQS at `http://localhost:4566` or provide the location of your SQS Queue and your credentials using the environment variables that have been described below.

## Using the APIs

Once the app has started, a number of APIs will be made available for you to send a set of predefined sample requests.

You can find an [Insomnia export for these APIs here](./docs/Insomnia.json).

## Environment Variables

| Variable                | Description                            |
| ----------------------- | -------------------------------------- |
| `SQS_ENDPOINT`          | The SQS endpoint to use for your queue |
| `SQS_ACCESS_KEY_ID`     | The access key for your queue          |
| `SQS_SECRET_ACCESS_KEY` | The secret access key for your queue   |
| `SQS_QUEUE_URL`         | The URL of your queue                  |
