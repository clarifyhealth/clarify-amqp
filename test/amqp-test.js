var expect = require('chai').expect;
var sinon = require('sinon');
var logger = require('clarify-logger');
var appRoot = require('app-root-path');
var Promise = require('bluebird');

var amqp = require(appRoot + '/clarify-amqp.js');

describe('AMQP', function () {
  describe('#init', function () {
    var initEventSpy = sinon.spy();
    amqp.events.on('initialized', initEventSpy);

    before(function (done) {
      amqp.init().then(done);
    });

    it('should set up ClarifyQueue', function () {
      expect(amqp.clarifyQueue).to.equal('ClarifyQueue');
    });

    it('should set up HospitalQueue', function () {
      expect(amqp.hospitalQueue).to.equal('HospitalQueue');
    });

    it('should set up PatientQueue', function () {
      expect(amqp.patientQueue).to.equal('PatientQueue');
    });

    it('should set up a topic exchange', function () {
      expect(amqp.topicExchange).to.equal('TopicExchange');
    });

    it("should emit 'initialized' event", function () {
      expect(initEventSpy.called).to.be.true;
      expect(initEventSpy.calledOnce).to.be.true;
    });
  });

  describe('#consuming', function () {
    before(function (done) {
      amqp.init().then(() => {
        return amqp.channel.purgeQueue(amqp.patientQueue);
      }).then(() => done());
    });

    it('consumes a message', function (done) {
      function processMessage(msg) {
        logger.info('Received message: %s', msg.content);
        amqp.channel.ack(msg);
      }

      var processMessageSpy = sinon.spy(processMessage);
      amqp.registerHandler(amqp.patientQueue, processMessageSpy);
      amqp.startConsuming(amqp.patientQueue);
      amqp.publish('test.message', '{ "x": 2 }',
            'TestModel',
                   'New');

      setTimeout(function() {
        expect(processMessageSpy.calledOnce).to.be.true;
        done();
      }, 1000);
    });
  });

  describe('#resiliancy', function () {
    this.timeout(3000);
    var reconnectSpy = sinon.spy();
    var republishSpy;

    var sandbox;
    before(function (done) {
      sandbox = sinon.sandbox.create();
      amqp.init().then(Promise.map(
        [
          amqp.clarifyQueue,
          amqp.hospitalQueue,
          amqp.patientQueue
        ],
        function (queueName) {
          return amqp.channel.purgeQueue.call(amqp.channel,
            queueName);
        })).then(function () {

        // after initial connect, force amqp publish to fail.
        // this should publish the message to a local offline queue.
        // then reconnect to the broker and publish the message.

        // force amqplib publish to fail
        sinon.stub(amqp.channel, 'publish', function () {
          throw 'SimulatedError';
        });
        amqp.events.on('initialized', reconnectSpy);

        amqp.publish('test.message', '{ "x": 2 }',
            'TestModel',
            'New')
          .then(function () {
            sandbox.restore();
            republishSpy = sinon.spy(amqp, 'publish');
            done();
          });
      });
    });

    beforeEach(function () {
      sandbox = sinon.sandbox.create();
    });

    afterEach(function () {
      sandbox.restore();
    });

    it(
      'should store the message in an offline queue if publish fails',
      function () {
        expect(amqp.getOfflineQueueLength()).to.equal(1);
      });

    it('should reconnect automatically on connection loss',
      function (done) {
        setTimeout(function () {
          expect(reconnectSpy.called).to.be.true;
          expect(reconnectSpy.calledOnce).to.be.true;
          done();
        }, 2800);
      });

    it('should have popped the message off the offline queue',
      function (done) {
        setTimeout(function () {
          expect(amqp.getOfflineQueueLength()).to.equal(0);
          done();
        });
      }, 1800);

    it('should have republished the message when reconnecting',
      function (done) {
        setTimeout(function () {
          expect(republishSpy.called).to.be.true;
          expect(republishSpy.calledOnce).to.be.true;
          done();
        });
      }, 1800);
  });
});
