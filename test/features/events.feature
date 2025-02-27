Feature: Events are emitted with the correct parameters and metadata

  Scenario: Error event without a message is emitted with undefined as the second parameter
    Given a consumer that will encounter an error without a message
    When the consumer starts
    Then an error event should be emitted with undefined as the second parameter
    And the event should include metadata with queueUrl

  Scenario: Error event with a message is emitted with the message as the second parameter
    Given a test message is sent to the SQS queue for events test
    And a consumer that will encounter an error with a message
    When the consumer starts
    Then an error event should be emitted with the message as the second parameter
    And the event should include metadata with queueUrl

  Scenario: Message events are emitted with metadata
    Given a test message is sent to the SQS queue for events test
    And a consumer that processes messages normally
    When the consumer starts
    Then message_received event should be emitted with metadata
    And message_processed event should be emitted with metadata

  Scenario: Empty queue event is emitted with metadata
    Given an empty SQS queue
    And a consumer that processes messages normally
    When the consumer polls an empty queue
    Then empty event should be emitted with metadata

  Scenario: Started and stopped events are emitted with metadata
    Given a consumer that processes messages normally
    When the consumer starts and stops
    Then started event should be emitted with metadata
    And stopped event should be emitted with metadata 