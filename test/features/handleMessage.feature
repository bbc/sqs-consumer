Feature: When handleMessage is used, messages are consumed without error

  Scenario: A message is consumed from SQS
    Given a message is sent to the SQS queue
    Then the message should be consumed without error

  Scenario: Multiple messages are consumed from SQS
    Given messages are sent to the SQS queue
    Then the messages should be consumed without error