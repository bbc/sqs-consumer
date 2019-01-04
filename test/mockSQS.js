'use strict';

class MockSQS {
  constructor() { }
  receiveMessage() { }
  deleteMessage() { }
  changeMessageVisibility() { }
}

module.exports = MockSQS;
