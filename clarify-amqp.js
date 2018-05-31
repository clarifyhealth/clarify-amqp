'use strict';

var amqp = require('amqplib');
var fs = require('fs');
var util = require('util');
var Promise = require('bluebird');
var _ = require('lodash');
var EventEmitter = require('events').EventEmitter;
var logger = require('clarify-logger');
var appRoot = require('app-root-path');
var config = require('config');
var changeCase = require('change-case');

var amqpConfig = {
  "protocol": "amqp",
  "server": "ec2-52-32-108-176.us-west-2.compute.amazonaws.com",
  "vhost": "",
  "user": "clarify",
  "password": "clarify",
  "application": "UNDEFINED"
};
if (config.has('clarify-amqp')) {
  amqpConfig = config.util.extendDeep(amqpConfig, config.get('clarify-amqp'));
}

var amqpURL = util.format('%s://%s:%s@%s/%s',
			  amqpConfig.protocol,			  
			  amqpConfig.user, amqpConfig.password,
			  amqpConfig.server, amqpConfig.vhost);

var isConnected = false;
var messageHandlers = {};

var topicExchange = 'TopicExchange';
var queues = ['ClarifyQueue', 'HospitalQueue', 'PatientQueue'];

module.exports.events = new EventEmitter();

/**
 * Initialize AMQP connection, channel, exchange, and queues.
 */
var connection = null;

// close connection on SIGINT
process.on('SIGINT', function () {
  if (isConnected) {
    logger.info('[AMQP] Closing connection.');
    connection.removeListener('close', reconnect);
    connection.close().then(process.exit);
  }
});


function init() {
  return amqp.connect(amqpURL)
    .then(
      function (conn) {
        logger.info('[AMQP] Connected to %s', amqpURL);
        connection = conn;
        isConnected = true;

        connection.on("error", function (err) {
          if (err.message !== "Connection closing") {
            logger.error("[AMQP] conn error", err.message);
          }
        });
        connection.on('close', reconnect);


        return connection.createConfirmChannel();
      }, reconnect)
    .then(
      function (chan) {
        if (isConnected) {
          logger.info('[AMQP] Created channel.');
          module.exports.channel = chan;

          // create topic exchange
          return chan.assertExchange(topicExchange, 'topic', {
              durable: true
            })
            .then(function () {
              logger.info('[AMQP] Created topic exchange: %s',
                topicExchange);

              module.exports.topicExchange = topicExchange;

              function initQueue(queueName) {
                // create queue to receive messages
                return chan.assertQueue(queueName, {
                    durable: true
                  })
                  .then(function (qInfo) {
                    logger.info('[AMQP] Created queue: %s', queueName);
                    // bind queue to topic exchange to receive all messages
                    return chan.bindQueue(queueName, topicExchange, '#').then(
                      function () {
                        module.exports[changeCase.camelCase(queueName)] =
                          queueName;
                      });
                  });
              }

              return Promise.map(queues, initQueue)
                .error(closeConnectionOnError)
                .then(function () {
                  module.exports.events.emit('initialized');
                });
            }, reconnect);
        }
      }, closeConnectionOnError)
    .then(publishOfflineQueue, closeConnectionOnError);
};

function closeConnectionOnError(err) {
  if (!err || !isConnected) return false;

  logger.error("[AMQP] error", err);
  connection.close();
  return true;
}

function reconnect() {
  {
    logger.error("[AMQP] reconnecting in 1 second.");
    isConnected = false;
    return setTimeout(init, 1000);
  }
}

function publishCallback(err) {
  if (err !== null) {
    logger.error('[AMQP] Publish error: %s', JSON.stringify(err));
  } else {
    // logger.info('[AMQP] Publish confirmed');
  }
}

module.exports.init = init;

var offlinePubQueue = [];

/**
 * Publish a message to the default topic exchange.
 */
module.exports.publish = function (routingKey, data, model, action,
  confirmCallback) {
  module.exports.channel.publishAsync = Promise.promisify(module.exports.channel.publish);
  return module.exports.channel.publishAsync(topicExchange, routingKey,
    new Buffer(JSON.stringify(data)), {
      options: {
        persistent: true
      },
      headers: {
        source: amqpConfig.application,
        model: model,
        action: action
      }
    }
  ).catch(
    function (err) {
      if (err !== null) {
        offlinePubQueue.push({
          'routingKey': routingKey,
          'data': data,
          'model': model,
          'action': action
        });
        logger.error('[AMQP] Publish error: %s', JSON.stringify(err));
        if (isConnected)
          return connection.close();
      }
      return Promise.resolve(true);
    });
};

function publishOfflineQueue() {
  if (isConnected) {
    while (true) {
      var m = offlinePubQueue.shift();
      if (!m) break;
      module.exports.publish(m.routingKey, m.data, m.model, m.action);
    }
  }
}

module.exports.getOfflineQueueLength = function () {
  return offlinePubQueue.length;
};

/**
 * Register a message hander and start consuming messages off a queue.
 */
module.exports.registerHandler = function (queue, handler, model, action) {
  if (typeof model === 'undefined')
    model = '_any';
  if (typeof action === 'undefined')
    action = '_any';

  if (typeof messageHandlers[queue] === 'undefined')
    messageHandlers[queue] = {};
  if (typeof messageHandlers[queue][model] === 'undefined')
    messageHandlers[queue][model] = {};

  var asyncHandler = handler;
  if (typeof handler === 'function')
    asyncHandler = Promise.promisify(handler);

  messageHandlers[queue][model][action] = asyncHandler;
};

// Generically handles CRUD for loopback models
function processModelMessage(msg) {
  var app = require(appRoot + '/server/server.js');
  var model = app.models[msg.properties.headers.model];
  var source = app.models[msg.properties.headers.source];

  if (source === amqpConfig.application)
  {
    logger.info('Skipping %s: %s message because source is same as consumer.',
      msg.properties.headers.model, msg.content);
    module.exports.channel.ack(msg);
  }

  if (typeof model === 'undefined') {
    module.exports.channel.reject(msg, false);
    Promise.reject(new Error(util.format(
      'Unable to process %s message using default model handler: %s',
      msg.properties.headers.model, msg.content)));
    return;
  }

  if (msg.properties.headers.action === 'Delete') {
    logger.info('Deleting %s: %s', msg.properties.headers.model, msg.content);
    app.models[msg.properties.headers.model].destroyById(msg.content,
      function (err, instance) {
        if (err)
          logger.error(err);

        module.exports.channel.ack(msg);
      });
  } else {
    var modelInstance = JSON.parse(msg.content);
    logger.info('Upserting %s: %s', msg.properties.headers.model, msg.content);
    app.models[msg.properties.headers.model].upsert(modelInstance,
      function (err, instance) {
        if (err)
          logger.error(err);

        module.exports.channel.ack(msg);
      });
  }
}

function processMessage(queue, msg) {
  var model = '_any';
  var action = '_any';

  if (typeof msg.properties.headers.model === 'string')
    model = msg.properties.headers.model;
  if (typeof msg.properties.headers.action === 'string')
    action = msg.properties.headers.action;

  // identifiy handler by looking for most specific registered handlers first
  var handler;
  if (typeof messageHandlers[queue][model] !== 'undefined' &&
    typeof messageHandlers[queue][model][action] !== 'undefined')
    handler = messageHandlers[queue][model][action];
  else if (typeof messageHandlers[queue][model] !== 'undefined' &&
    typeof messageHandlers[queue][model]['_any'] !== 'undefined')
    handler = messageHandlers[queue][model]['_any'];
  else if (typeof messageHandlers[queue][model] === 'undefined' &&
    typeof messageHandlers[queue]['_any'][action] !== 'undefined')
    handler = messageHandlers[queue]['_any'][action];
  else
    handler = messageHandlers[queue]['_any']['_any'];

  if (typeof handler === 'function')
  // call custom handler
    return handler(msg);
  else
  // call generic model handler
    return processModelMessage(msg);
}

module.exports.startConsuming = function (queue) {
  var queueMessageHandler = _.partial(processMessage, queue, _);
  return module.exports.channel.consume(queue, queueMessageHandler);
};
