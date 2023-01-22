Feature: When handleMessageBatch is used, messages are consumed without error

  Scenario: A message batch is consumed from SQS
    Given a message batch is sent to the SQS queue
    Then the message batch should be consumed without error