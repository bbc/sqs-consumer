var Consumer = require('..');
var assert = require('assert');
var sinon = require('sinon');

describe('Consumer', function () {
  var consumer;
  var handleMessage;
  var sqs;
  var response = {
    Messages: [{
      ReceiptHandle: 'receipt-handle',
      MessageId: '123',
      Body: 'body'
    }]
  };

  beforeEach(function () {
    handleMessage = sinon.stub();
    sqs = sinon.mock();
    sqs.receiveMessage = sinon.stub().yields(null, response);
    sqs.receiveMessage.onSecondCall().returns();
    sqs.deleteMessage = sinon.stub().yields(null);
    consumer = new Consumer({
      queueUrl: 'some-queue-url',
      region: 'some-region',
      handleMessage: handleMessage,
      waitTime: 10,
      sqs: sqs
    });
  });

  it('requires a queueUrl to be set', function () {
    assert.throws(function () {
      new Consumer({
        region: 'some-region',
        handleMessage: handleMessage
      });
    });
  });

  it('requires an AWS region to be set', function () {
    assert.throws(function () {
      new Consumer({
        queueUrl: 'some-queue-url',
        handleMessage: handleMessage
      });
    });
  });

  it('requires a handleMessage function to be set', function () {
    assert.throws(function () {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url'
      });
    });
  });

  describe('.start', function () {
    it('calls the handleMessage function when a message is received', function () {
      consumer.start();

      sinon.assert.calledWith(handleMessage, response.Messages[0]);
    });

    it('deletes the message when the handleMessage callback is called', function () {
      handleMessage.yields(null);

      consumer.start();

      sinon.assert.calledWith(sqs.deleteMessage, {
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle'
      });
    });

    it('doesn\'t delete the message when a processing error is reported', function () {
      handleMessage.yields(new Error('Processing error'));

      consumer.on('error', function () {
        // ignore the error
      });

      consumer.start();

      sinon.assert.notCalled(sqs.deleteMessage);
    });

    it('waits before consuming new messages', function (done) {
      sqs.receiveMessage.onSecondCall().yields(null, response);

      consumer.start();

      setTimeout(function () {
        sinon.assert.calledTwice(handleMessage);
        done();
      }, 11);
    });

    it('fires an error event when an error occurs receiving a message', function (done) {
      var receiveErr = new Error('Receive error');

      sqs.receiveMessage.yields(receiveErr);

      consumer.on('error', function (err) {
        assert.equal(err, receiveErr);
        done();
      });

      consumer.start();
    });

    it('fires an error event when an error occurs deleting a message', function (done) {
      var deleteErr = new Error('Delete error');

      handleMessage.yields(null);
      sqs.deleteMessage.yields(deleteErr);

      consumer.on('error', function (err) {
        assert.equal(err, deleteErr);
        done();
      });

      consumer.start();
    });

    it('fires an error event when an error occurs processing a message', function (done) {
      var processingErr = new Error('Processing error');

      handleMessage.yields(processingErr);

      consumer.on('error', function (err) {
        assert.equal(err, processingErr);
        done();
      });

      consumer.start();
    });

    it('fires a message_received event when a message is received', function (done) {
      consumer.on('message_received', function (message) {
        assert.equal(message, response.Messages[0]);
        done();
      });

      consumer.start();
    });

    it('fires a message_processed event when a message is successfully deleted', function (done) {
      handleMessage.yields(null);

      consumer.on('message_processed', function (message) {
        assert.equal(message, response.Messages[0]);
        done();
      });

      consumer.start();
    });

    it('doesn\'t consumer more messages when called multiple times', function () {
      consumer.start();
      consumer.start();
      consumer.start();

      sinon.assert.calledOnce(sqs.receiveMessage);
    });
  });

  describe('.stop', function () {
    it('stops the consumer polling for messages', function (done) {
      sqs.receiveMessage.onSecondCall().yields(null, response);

      consumer.start();
      consumer.stop();

      setTimeout(function () {
        sinon.assert.calledOnce(handleMessage);
        done();
      }, consumer.waitTime + 1);
    });
  });
});