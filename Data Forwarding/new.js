const mqtt = require('mqtt');
const axios = require('axios');
const { find, map } = require('lodash');
const forge = require("node-forge");
const moment = require("moment");

/**
 * @function convert
 * @param {Number} data 
 * @returns data rounded to 2 decimal places
 */
const convert = (data, precision = 2) => Number(Number(data).toFixed(precision));

/* CONSTANTS */

// OCEMS API Configuration
const OCEMS_CONFIG = {
    // Live Server
    liveServer: 'http://hspcbcems.nic.in/hrcpcb-api/api',

    // Test Server
    testServer: 'http://173.208.244.178/hrcpcb-api/api',
};

// Station and Device Mapping
// Configure your stations and their devices here
const STATION_CONFIGS = {
    'MG2511FM_E1': {
        industryId: 'industry_6038',
        stationId: 'station_13289',
        deviceId: 'device_12478',
        token: 'aCKyLns57idxz4Y033DrEnp953Ps55C2zsJzIF6jYw='
    },
    'MG2511FM_E2': {
        industryId: 'industry_6038',
        stationId: 'station_13275',
        deviceId: 'device_12398',
        token: 'ozdR-RSE2-rgOWvk1BpGdQWv4GIDjUCFBwjQNdnd0o4='
    }
};

const paramSensorMapping = {
    'MG2511FM_E1': {
        // tag -> parameter key
        'TOTAL': 'flow_totalizer'
    },
    'MG2511FM_E2': {
        // tag -> parameter key
        'TOTAL': 'flow_inlet'
    },
}

let isRestarted = true;

const devices = Object.keys(STATION_CONFIGS)
const cachedSlopeAndIntercept = {}
Object.keys(STATION_CONFIGS).forEach((deviceId) => {
    cachedSlopeAndIntercept[deviceId] = {  }
})

/**
 * Generates the API signature.
 *
 * @param {string} tokenId      - Your API token ID
 * @param {string} publicKeyPem - RSA public key in PEM format
 * @returns {string}            - Base64-encoded encrypted signature
 */
function generateSignature(tokenId, publicKeyPem) {
    // Step 1: Build the message — token_id + "$*" + current_timestamp (IST)
    const timestamp = moment().utcOffset('+05:30').format("YYYY-MM-DD HH:mm:ss.SSS");
    const message = `${tokenId}$*${timestamp}`;

    console.log("Timestamp  :", timestamp);
    console.log("Message    :", message);

    // Step 2: Convert message to UTF-8 bytes
    const messageBytes = forge.util.createBuffer(message, 'utf8').getBytes();

    // Step 3: Encrypt using RSA-OAEP with SHA-256 (MGF1 with SHA-256)
    const publicKey = forge.pki.publicKeyFromPem(publicKeyPem);
    const encryptedBytes = publicKey.encrypt(messageBytes, "RSA-OAEP", {
        md: forge.md.sha256.create(),
        mgf1: {
            md: forge.md.sha256.create(),
        }
    });

    // Step 4: Base64-encode the encrypted bytes (utf-8 safe)
    const signature = forge.util.encode64(encryptedBytes);

    console.log("Signature  :", signature);

    return signature;
}

function encryptPayload(payload, tokenId) {

    // Step 1: Derive the AES key — hash the token ID with SHA-256 (produces 256-bit key)
    const md = forge.md.sha256.create();
    md.update(tokenId, "utf8");
    const aesKey = md.digest().bytes(); // raw binary, 32 bytes = 256 bits

    // Step 2: Create AES cipher in ECB mode using the hashed key
    const cipher = forge.cipher.createCipher("AES-ECB", aesKey);

    // Step 3: Encrypt the payload with PKCS#7 padding (pads to AES block size of 16 bytes)
    const payloadStr =
        typeof payload === "object" ? JSON.stringify(payload) : String(payload);

    console.log("Payload      :", payloadStr);

    cipher.start(); // ECB mode requires no IV
    cipher.update(forge.util.createBuffer(payloadStr, "utf8"));
    cipher.finish(); // applies PKCS#7 padding automatically

    const encryptedBytes = cipher.output.bytes();

    // Step 4: Base64-encode the binary ciphertext (utf-8 safe)
    const encryptedPayload = forge.util.encode64(encryptedBytes);

    console.log("Encrypted    :", encryptedPayload);

    return encryptedPayload;
}

const PUBLIC_KEY_PEM = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RH6ZyIj3MXVPjbfirf/
fP/Zqhh1J1EoHPEez/cTfpG5JR0STcTbdLDqBLwZ5ru3eHfVd38e+y+IxYpmYVt9
nq8OVrTArfU6sa9Q5NeAQBcNaDFf2rlRh0Dcxl+YWqfSM5CdZAJ8xLxbw7BmI2ZD
6MXHInNBJHKSFxM/R4FK4Zg8ymH7K8/k69lg+UCT16HBJ00qgwAd9PBcz6XUtFGf
FsFOwoIgt1MOVl9pwU5a5M5oM0TAk15aMiCGJ7en7jEFMm52l0WklthBtMF3Db60
0aDC1EwAX9cbMmKk7f8UkkVpKbW1Kec8edqwb6n00PUg2o86DyeoAysiQAji3Hfr
YwIDAQAB
-----END PUBLIC KEY-----`;

/**
 * Upload data to OCEMS API
 */
const uploadToServer = async (device, payload) => {
    try {
        const { token, deviceId } = STATION_CONFIGS[device]
        const authHeader = `Basic ${token}`;
        console.log(`Data: ${JSON.stringify(payload)}`);
        console.log('Generated Signature:', generateSignature(token, PUBLIC_KEY_PEM));

        const response = await axios.post('https://cems.cpcb.gov.in/v1.0/industry/data', payload, {
            headers: {
                'Authorization': authHeader,
                'X-Device-Id': deviceId,
                'signature': generateSignature(token, PUBLIC_KEY_PEM),
                'Content-Type': 'application/json'
            },
            timeout: 10000 // 10 second timeout
        });

        console.log('OCEMS API Response:', response.data);
        return response.data;

    } catch (error) {
        if (error.response) {
            // Server responded with error
            console.error('OCEMS API Error Response:', error.response.data);
            console.error('Status Code:', error.response.status);
            
            // Handle specific error codes from OCEMS API
            const status = error.response.data?.status;
            switch(status) {
                case 10:
                    console.error('ERROR: Invalid API Key');
                    break;
                case 11:
                    console.error('ERROR: Invalid JSON format');
                    break;
                case 101:
                    console.error('ERROR: Invalid industry ID');
                    break;
                case 102:
                    console.error('ERROR: Invalid station ID');
                    break;
                case 108:
                    console.error('ERROR: Invalid device ID');
                    break;
                default:
                    console.error('ERROR: Unknown error from OCEMS API');
            }
        } else if (error.request) {
            // Request made but no response
            console.error('OCEMS API Error: No response received', error.message);
        } else {
            // Error in request setup
            console.error('OCEMS API Error:', error.message);
        }
        throw error;
    }
};

function buildPayload(dataPacket) {

    try {
        
        const payload = [];
        for (let sensor of Object.keys(paramSensorMapping[dataPacket.device])) {
            
            const tag = find(dataPacket.data, { tag: sensor })
            if(!tag) continue;

            const { m, c, unit, location } = cachedSlopeAndIntercept[dataPacket.device][sensor]

            // apply calibration
            // let value = (m * tag.value) + c;
            let value = tag.value;
        
            payload.push({
                stationId: STATION_CONFIGS[dataPacket.device].stationId,
                device_data: [
                    {
                        deviceId: STATION_CONFIGS[dataPacket.device].deviceId,
                        params: [{
                            parameter: paramSensorMapping[dataPacket.device][sensor],
                            value: convert(value),
                            unit,
                            timestamp: dataPacket.time,
                            flag: 'C'
                        }],
                        diag_params: [{
                            parameter: paramSensorMapping[dataPacket.device][sensor],
                            value: convert(value),
                            unit,
                            timestamp: dataPacket.time,
                            flag: 'U'
                        }]
                    }
                ],
                latitude: location ? location.latitude : 0,
                longitude: location ? location.longitude : 0
            });
        }

        return { data: payload };
        
    } catch (error) {
        console.error('Error building payload:', error);
        throw error;
    }
}

/* Main Parser */
module.exports = async (app) => {

    const mqttConfig = JSON.parse(JSON.stringify(app.config.MQTTConfig));
    mqttConfig.clientId = `OCEMS_Uploader_${Date.now()}`;
    
    let mqttClient = mqtt.connect(mqttConfig);

    mqttClient.on('connect', async () => {
        console.log("Connected to MQTT!");
        for(let devID of devices) {
            mqttClient.subscribe(`devicesIn/${devID}/data`);
            console.log('Subscribed to', devID);
        }
    });

    mqttClient.on('error', (error) => {
        console.error('OCEMS Uploader - Error connecting MQTT...', error);
    });

    mqttClient.on('message', async (topic, message) => {
        try {

            const dataPacket = JSON.parse(message);

            if(!devices.includes(dataPacket.device)) return;

            if(!moment(dataPacket.time).isAfter(moment().subtract(7, 'days'))) return;

            console.log('Delhi Pollution Forwarder received ===>', dataPacket)

            if(isRestarted) {
                const devs = await app.db.models.Device.find({ devID: { $in: devices } }).select('devID params unit location').lean();
                if(!devs || !devs.length) throw new Error('device not found!');
                for(let device of devs) {
                    const sensors = Object.keys(paramSensorMapping[device.devID]);
                    for(let sensor of sensors) {

                        const slope = find(device.params[sensor], { paramName: 'm' });
                        const m = slope? slope.paramValue : 1;

                        const intercept = find(device.params[sensor], { paramName: 'c' });
                        const c = intercept? intercept.paramValue : 0;

                        let unit = '';
                        if(device.unit && device.unit[sensor]) {
                            if(device.unit[sensor].length >= 2) unit = device.unit[sensor][1];
                            else unit = device.unit[sensor][0];
                        }
                        cachedSlopeAndIntercept[device.devID][sensor] = { m, c, unit, location: device.location };
                    }
                }
                isRestarted = false;
            }

            /*****************************************************/

            const payload = buildPayload(dataPacket);
            if(payload.data.length === 0) return;
            console.dir(payload, { depth: Infinity });

            const encryptedPayload = encryptPayload(payload, STATION_CONFIGS[dataPacket.device].token);

            /*****************************************************/

            // //* Upload to API
            const response = await uploadToServer(dataPacket.device, encryptedPayload);
            console.log('response for', dataPacket.device, response);

            if (response.status === 1) {
                console.log(`✓ Successfully uploaded data for station, device ${STATION_CONFIGS[dataPacket.device].deviceId}`);                
            } else {
                console.error(`✗ Failed to upload data. Response:`, response);
            }

            console.log('\n\n--------------------------------------------------\n\n');

            /*****************************************************/

        } catch (error) {
            console.error('Uploader Error:', error);
        }
    });
};