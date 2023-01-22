Feature: When handleMessage is used, messages are consumed without error

  Scenario: A message is consumed from SQS
    Given a message is sent to the SQS queue
    When SQS Consumer is started
    Then the message should be consumed without error