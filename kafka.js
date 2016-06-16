var WebSocketClient = require('websocket').client;
var wsClient = new WebSocketClient();
var kafka = require('kafka-node'),
    Producer = kafka.Producer;
var config = require('./config.js');




var kafkaClient = new kafka.Client(config.kafka.zookeeper_url);
var producer = new Producer(kafkaClient);


producer.on('ready', function () {

    wsClient.on('connectFailed', function (error) {
        console.log('Connect Error: ' + error.toString());
    });

    wsClient.on('connect', function (connection) {
        console.log('WebSocket Client Connected');
        connection.on('error', function (error) {
            console.log("Connection Error: " + error.toString());
        });
        connection.on('close', function () {
            console.log('echo-protocol Connection Closed');
        });
        connection.on('message', function (message) {
            if (message.type === "utf8") {
                var stuff = message.utf8Data;
                var payload = [{topic: 'meetup_rsvp', messages: stuff, partition: 0}];
                console.log(payload);
                producer.send(payload, function (err, data) {
                    console.log(err);
                    console.log(data);
                });
            }
        });

    });

    wsClient.connect('ws://stream.meetup.com/2/rsvps');

});

producer.on('error', function (err) {
    console.log(err);
});