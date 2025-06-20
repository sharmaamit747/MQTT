const mqtt = require('mqtt');
const mysql = require('mysql2');

// --- 1. MySQL Connection Setup ---
const db = mysql.createConnection({
    host: 'localhost',
    user: 'u847688019_mqtt',
    password: 'u847688019_mqtt',
    database: 'u847688019_mqtt',
});

db.connect(err => {
    if (err) {
        console.error('❌ MySQL connection error:', err);
        process.exit(1);
    }
    console.log('✅ Connected to MySQL database');
});

// --- 2. MQTT Setup ---
const mqttBroker = 'mqtt://help.rank2top.com';

const topics = {
    led: {
        set: 'led/status',
        get: 'led/status/get',
        feedback: 'led/feedback',
    },
    fan: {
        set: 'fan/status',
        get: 'fan/status/get',
        feedback: 'fan/feedback',
    }
};

const mqttClient = mqtt.connect(mqttBroker);

mqttClient.on('connect', () => {
    const allTopics = [
        topics.led.set,
        topics.led.get,
        topics.led.feedback,
        topics.fan.set,
        topics.fan.get,
        topics.fan.feedback,
    ];

    mqttClient.subscribe(allTopics, err => {
        if (!err) console.log('✅ Subscribed to all topics');
        else console.error('❌ Subscription error:', err);
    });
});

mqttClient.on('message', (topic, message) => {
    try {
        const payload = JSON.parse(message.toString());
        if (topics.led) {
            const {
                device_id,
                status,
                message: logMessage,
                time = null,
                ...rest
            } = payload;
            if (!device_id) return;
            // --- LED Device Handling ---
            if (topic === topics.led.set && status) {
                const q = 'SELECT status FROM led_devices WHERE device_id = ?';
                db.query(q, [device_id], (err, results) => {
                    if (err) throw err;

                    if (results.length === 0) {
                        db.query('INSERT INTO led_devices (device_id, status) VALUES (?, ?)', [device_id, status]);
                    } else if (results[0].status !== status) {
                        db.query('UPDATE led_devices SET status = ? WHERE device_id = ?', [status, device_id]);
                    }
                });
            }

            if (topic === topics.led.get) {
                const q = 'SELECT status, feed FROM led_devices WHERE device_id = ?';
                db.query(q, [device_id], (err, results) => {
                    if (err) throw err;
                    const response = results.length > 0 ?
                        {
                            device_id,
                            status: results[0].status,
                            feed: results[0].feed
                        } :
                        {
                            device_id,
                            status: "UNKNOWN",
                            feed: null
                        };
                    mqttClient.publish(led/status/response/${device_id}, JSON.stringify(response));
                });
            }

            if (topic === topics.led.feedback && logMessage) {
                const updateFeed = 'UPDATE led_devices SET feed = ?, time = ? WHERE device_id = ?';
                db.query(updateFeed, [logMessage, time, device_id]);

                mqttClient.publish(
                    led/feedback/app/${device_id},
                    JSON.stringify({
                        device_id,
                        message: logMessage,
                        time
                    })
                );
            }
        }
       if (topics.fan) {
    const {
        deviceId,
        state,
        message: logMessage,
        time = null,
        ...rest
    } = payload;

    if (!deviceId) return;

    // --- FAN Device Handling ---
    if (topic === topics.fan.set) {
        if (state !== undefined) rest.state = state;

        const selectQuery = 'SELECT * FROM fan_devices WHERE deviceId = ?';
        db.query(selectQuery, [deviceId], (err, results) => {
            if (err) {
                console.error('❌ Fan select error:', err);
                return;
            }

            if (results.length === 0) {
                // INSERT new record
                const insertFields = ['deviceId', ...Object.keys(rest)];
                const insertValues = [deviceId, ...Object.values(rest)];
                const placeholders = insertFields.map(() => '?').join(', ');

                const insertQuery = INSERT INTO fan_devices (${insertFields.join(', ')}) VALUES (${placeholders});
                db.query(insertQuery, insertValues, err => {
                    if (err) {
                        console.error('❌ Fan insert error:', err);
                        return;
                    }
                    mqttClient.publish(fan/feedback/app/${deviceId}, JSON.stringify({
                        deviceId,
                        ...rest
                    }));
                    console.log(✅ New fan device inserted: ${deviceId});
                });
            } else {
                // UPDATE only changed fields
                const dbRow = results[0];
                const updates = [];
                const values = [];

                for (const key in rest) {
                    if (rest[key] != dbRow[key]) {
                        updates.push(${key} = ?);
                        values.push(rest[key]);
                    }
                }

                if (updates.length > 0) {
                    const updateQuery = UPDATE fan_devices SET ${updates.join(', ')} WHERE deviceId = ?;
                    values.push(deviceId);

                    db.query(updateQuery, values, err => {
                        if (err) {
                            console.error('❌ Fan update error:', err);
                            return;
                        }
                        mqttClient.publish(fan/feedback/app/${deviceId}, JSON.stringify({
                            deviceId,
                            ...rest
                        }));
                        console.log(✅ Fan device updated: ${deviceId});
                    });
                } else {
                    console.log(ℹ️ No changes detected for fan device: ${deviceId});
                }
            }
        });
    }

    if (topic === topics.fan.get) {
        const q = 'SELECT * FROM fan_devices WHERE deviceId = ?';
        db.query(q, [deviceId], (err, results) => {
            if (err) {
                console.error('❌ Fan GET error:', err);
                return;
            }

            const response = results.length > 0
                ? { deviceId, ...results[0] }
                : { deviceId, status: "UNKNOWN" };

            mqttClient.publish(fan/status/response/${deviceId}, JSON.stringify(response));
        });
    }

    if (topic === topics.fan.feedback && logMessage) {
        const updateFeed = 'UPDATE fan_devices SET feed = ?, time = ? WHERE deviceId = ?';
        db.query(updateFeed, [logMessage, time, deviceId]);

        mqttClient.publish(
            fan/feedback/app/${deviceId},
            JSON.stringify({
                deviceId,
                message: logMessage,
                time
            })
        );
    }
}





    } catch (err) {
        console.error('❌ Error parsing MQTT message:', err);
    }
});