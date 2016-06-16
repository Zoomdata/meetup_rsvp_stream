var websocket = require('websocket');
var solr = require('solr-client');
var underscore = require('underscore');
var config = require('./config');

var solrClient = solr.createClient(config.solr);
var wsClient = new websocket.client();

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

            var solrData = transform(JSON.parse(stuff));
            solrClient.add(solrData, function (err, res) {
                if (err) {
                    error(err);
                }
                if (res) {
                    success(res);
                }
            });


            solrClient.commit(function (err, res) {
                if (err) {
                    error(err);
                }
                if (res) {
                    success(res);
                }
            });
        }
    });

});


function transform(stuff) {
    var solrJson = {};

    if (stuff.venue) {
        solrJson['venue_name_s'] = stuff.venue.venue_name;
        solrJson['venue_id_i'] = stuff.venue.venue_id;
        solrJson['venue_lat_d'] = stuff.venue.lat;
        solrJson['venue_lon_d'] = stuff.venue.lon;
        solrJson['venue_lat_lon_p'] = stuff.venue.lat + "," + stuff.venue.lon;
    }

    var mTime = new Date(stuff.mtime);
    solrJson['mtime_dt'] = mTime;
    solrJson['rsvp_id_i'] = stuff.rsvp_id;

    if (stuff.event) {
        solrJson['event_name_s'] = stuff.event.event_name;
        solrJson['event_id_s'] = stuff.event.event_id;
        var eTime = new Date(stuff.event.time);
        // solrJson['event_time_td'] = eTime.toISOString();
        solrJson['event_time_dt'] = eTime;
        solrJson['event_url_s'] = stuff.event.event_url;
    }

    if (stuff.member) {
        solrJson['member_name_s'] = stuff.member.member_name;
        solrJson['photo_uri_s'] = stuff.member.photo;
        solrJson['member_id_i'] = stuff.member.member_id;
    }

    if (stuff.group) {
        solrJson['group_city_s'] = stuff.group.group_city;
        solrJson['group_country_s'] = stuff.group.group_country;
        solrJson['group_name_s'] = stuff.group.group_name;
        solrJson['group_id_i'] = stuff.group.group_id;
        solrJson['group_state_s'] = stuff.group.group_state;
        solrJson['group_topic_urlkeys_ss'] = underscore.map(stuff.group.group_topics, function (one) {
            return one.urlkey;
        });
        solrJson['group_topic_topic_names_ss'] = underscore.map(stuff.group.group_topics, function (one) {
            return one.topic_name;
        });
        solrJson['group_lat_d'] = stuff.group.group_lat;
        solrJson['group_lon_d'] = stuff.group.group_lon;
        solrJson['group_lat_lon_p'] = stuff.group.group_lat + "," + stuff.group.group_lon;
    }
    // console.log(solrJson);
    return solrJson;
}

function error(err) {

}

function success(obj) {

}

wsClient.connect('ws://stream.meetup.com/2/rsvps');

// var query = client.createQuery().q({'*': '*'});
//
// client.search(query, function (err, obj) {
//     if (err) {
//         console.log(err);
//     } else {
//         console.log(obj);
//     }
// });

