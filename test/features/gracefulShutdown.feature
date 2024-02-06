Feature: Graceful shutdown

  Scenario: Several messages in flight
    Given Several messages are sent to the SQS queue
    Then the application is stopped while messages are in flight
    Then the in-flight messages should be processed before stopped is emitted