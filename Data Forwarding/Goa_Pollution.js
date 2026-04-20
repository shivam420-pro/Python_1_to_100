const mqtt = require('mqtt');
const crypto = require('crypto');
const axios = require('axios');
const moment = require('moment-timezone');
const JSZip = require('jszip');
const FormData = require('form-data');
const { getDpBefore, getDPAfterButBefore } = require('../../../utils/apilayer/getDpServices');
const _ = require('lodash');

const metadata = 'SITE_ID,SITE_UID,MONITORING_UNIT_ID,ANALYZER_ID,PARAMETER_ID,PARAMETER_NAME,READING,UNIT_ID,DATA_QUALITY_CODE,RAW_READING,UNIX_TIMESTAMP,CALIBRATION_FLAG,MAINTENANCE_FLAG'
const configuration = {
    SV248FM_A1: {
        siteId: 'site_5422',
        encryptionKey: 'c2l0ZV81NDIyLHZlcl8yLjM=########',
        sensorParamMapping: {
            D0: 'COD',
            D3: 'TSS',
            D2: 'pH',
            D1: 'BOD',
            D4: 'TDS',

        },
        monitoringID: 'ETP',
        parametersMapping: {
            pH: { parameterID: 'parameter_13', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_10' },
            COD: { parameterID: 'parameter_83', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15' },
            BOD: { parameterID: 'parameter_84', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15' },
            TSS: { parameterID: 'parameter_85', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15' },
            TDS: { parameterID: 'parameter_209', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15' },
        },
    },
    SV248FM_B1: {
        siteId: 'site_5428',
        encryptionKey: 'c2l0ZV81NDI4LHZlcl8yLjM=########',
        sensorParamMapping: {
            D0: 'COD',
            D3: 'TSS',
            D2: 'pH',
            D1: 'BOD',
            D4: 'TDS',

        },
        monitoringID: 'ETP',
        parametersMapping: {
            pH: { parameterID: 'parameter_13', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_10'},
            COD: { parameterID: 'parameter_83', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15'},
            BOD: { parameterID: 'parameter_84', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15'},
            TSS: { parameterID: 'parameter_85', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15'},
            TDS: { parameterID: 'parameter_209', analyserID: 'analyzer_677', monitoringID: 'ETP', unitID: 'unit_15'},
        },
    },
}


const formatData = (data, deviceConfig) => {

    let dataToBeReturned = '';
    const { siteId, parametersMapping, encryptionKey } = deviceConfig;
    data.forEach((point) => {
        let data = `${siteId},${siteId},${parametersMapping[point.param].monitoringID},${parametersMapping[point.param].analyserID},${parametersMapping[point.param].parameterID},${point.param},${point.value},${parametersMapping[point.param].unitID},U,${point.value},${point.time - 19800},0,0\n`;
        dataToBeReturned = dataToBeReturned.concat(data);
    })

    dataToBeReturned = dataToBeReturned.substring(0, dataToBeReturned.length - 1) //Removed "\n" character from the end

    let dataToBeReturnedLength = dataToBeReturned.length;

    const BLOCK_SIZE = 32;

    if (dataToBeReturnedLength % BLOCK_SIZE != 0) {

        let lengthOfPadding = BLOCK_SIZE - dataToBeReturnedLength % BLOCK_SIZE; // determines how much padding is required
        dataToBeReturned = dataToBeReturned.padEnd(dataToBeReturnedLength + lengthOfPadding, '#');
    }

    console.log('data To Be Encrypted==>>', dataToBeReturned);
    const iv = crypto.randomBytes(16);
    const encryptedText = encryptUsingAES(encryptionKey, null, dataToBeReturned)
    return encryptedText;

}

const encryptUsingAES = (key, iv, data, isAutoPadding) => {
    if (!iv)
        iv = Buffer.alloc(16);

    key = key.slice(0, 32);

    const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(key), Buffer.from(iv));
    if (!isAutoPadding)
        cipher.setAutoPadding(false);

    const encryptedData = cipher.update(data, 'utf-8', 'base64') + cipher.final('base64');

    return `${encryptedData}`;
}


const sendRealTimeData = async (fileName, content, time, deviceConfig) => {
    const { siteId, encryptionKey } = deviceConfig;
    try {
        time = parseInt(`${time}000`) - 19800000

        const authMessage = `${siteId},ver_3.1,${moment(time).utc().add(5.5, 'h').format()},${encryptionKey}`

        const formData = new FormData();


        formData.append('file', content, { filename: fileName });

        const response = await axios({
            method: "post",
            url: "http://gspcbupload.glensweb.com/GSPCBServer/realtimeUpload",
            data: formData,
            headers: {
                Authorization: `Basic ${encryptUsingAES(encryptionKey, null, authMessage, true)}${Buffer.alloc(16).toString('base64')}`,
                siteId: siteId,
                Timestamp: moment(time).utc().add(5.5, 'h').format(),
                ...formData.getHeaders()
            },
        })

        console.log(response.data);
    } catch (error) {
        console.log({ siteId, error: error });
    }
}

const generateZipFile = async (metadata, data, deviceConfig) => {
    try {
        const zip = new JSZip();

        const { monitoringID, siteId } = deviceConfig;
        let uploadTime = moment.tz('Asia/Calcutta').format('YYYY MM DD HH mm ss');
        uploadTime = uploadTime.split(' ').join('');

        zip.file('metadata.csv', metadata);
        zip.file(`${siteId}_${monitoringID}_${uploadTime}.csv`, data);
        zip.name = `${siteId}_${monitoringID}_${uploadTime}.zip`;

        const content = await zip.generateAsync({ type: 'nodebuffer' });

        console.log('zip file generated successfully');
        return { content, fileName: zip.name };
    } catch (error) {
        return Promise.reject(error)
    }
}


const zipData = async (data, deviceConfig, time) => {
    try {
        let encryptedData = formatData(data, deviceConfig);
        const { content, fileName } = await generateZipFile(metadata, encryptedData, deviceConfig);
        await sendRealTimeData(fileName, content, time, deviceConfig);
        // }
    } catch (error) {
        return Promise.reject(error)
    }
}

module.exports = async (app) => {
    try {
        const mqttConfig = JSON.parse(JSON.stringify(app.config.MQTTConfig));
        mqttConfig.clientId = `GSPCB_MQTT_FORWARDER_${Date.now()}`;
        const mqttClient = mqtt.connect(mqttConfig);

        mqttClient.on('error', (error) => {
            console.log('Hooooo!! Error in GSPCB mqtt client', error);
            mqttClient.end();
        });

        mqttClient.on('connect', () => {
            console.log('GSPCB DataForwarder- Mqtt Connected');
            const devIDs = Object.keys(configuration);
            for (const devId of devIDs) {
                console.log(devId);
                const dataTopic = `$share/${app.config.sharedSubGroup}/devicesIn/${devId}/data`;
                mqttClient.subscribe(dataTopic);
            }
        });

        mqttClient.on('message', async (topic, message) => {
            try {
                const dataPacket = JSON.parse(message);
                const deviceID = topic.split('/')[1];
                const currentTimestamp = moment().unix() + 19800;

                const deviceConfig = configuration[deviceID];
                const sensorParamMapping = deviceConfig.sensorParamMapping;
                const sensorsToWatch = Object.keys(sensorParamMapping);

                const currentDataArray = []

                for (const packet of dataPacket.data) {

                    if (sensorsToWatch.includes(packet.tag)) {
                        const param = sensorParamMapping[packet.tag];
                        let value, isAlreadyCalibrated;
                        if (deviceConfig.totalizerTag && deviceConfig.totalizerTag == packet.tag) {
                            value = await getTotalizerValue(deviceID, packet.tag, packet.value);
                            isAlreadyCalibrated = true;
                        }
                        else {
                            value = packet.value;
                        }

                        if (deviceConfig.calibration && deviceConfig.calibration[packet.tag] && !isAlreadyCalibrated) {
                            let m = deviceConfig.calibration[packet.tag].m;
                            let c = deviceConfig.calibration[packet.tag].c;
                            value = value * m + c;
                            if (value < 0)
                                value = 0;
                            console.log(`${packet.tag} calibrated`);
                        }
                        currentDataArray.push({
                            param,
                            value,
                            time: currentTimestamp
                        });
                    }
                }
                if (currentDataArray.length) {
                    await zipData(currentDataArray, deviceConfig, currentTimestamp);
                }
            } catch (error) {
                console.log(error);
            }
        });
    } catch (err) {
        console.log(err);
    }
}

const getTotalizerValue = async (devID, sensor, value) => {
    try {

        // Fetch configuration for the specific site
        const devConfig = configuration[devID];

        let lastDayValue = devConfig.previousDayTotalizerValue;
        let isFirstDay = devConfig.isFirstDay;
        let timezone = 'Asia/Calcutta'

        // endTime(just one day previous)
        // currentISTTime(it is the current IST time)

        let currentIstTime = moment().tz(timezone).unix(); // current IST time

        // if server is restarted or even for the very first time, lastDayValue will be undefined
        // therefore initialize it with either by data before today in case of server restart
        // or with currentvalue if its the first time

        // Reset the previousDayTotalizerValue on every  new day

        if (!lastDayValue) {
            let previousDayStart = moment().tz(timezone).subtract(1, 'day').startOf('day').unix();
            let previousDayEnd = moment().tz(timezone).subtract(1, 'day').endOf('day').unix();


            lastDayValue = await getDpBefore(devID, sensor, previousDayStart, previousDayEnd);
            if (!lastDayValue) {
                let currentDayStart = moment().tz(timezone).startOf('day').unix();
                lastDayValue = await getDPAfterButBefore(devID, sensor, currentDayStart, currentIstTime);
                if (!lastDayValue) {
                    lastDayValue = value;
                    isFirstDay = true;
                    devConfig.isFirstDay = true;
                } else {
                    lastDayValue = lastDayValue.value;
                }
            } else {
                lastDayValue = lastDayValue.value;
            }
        }


        let finalValue;

        // check if it is the firstDay for the flow meter, and hence use the current value
        // for today flow, or else subtract the last day value from today value
        if (!isFirstDay)
            finalValue = parseFloat(value) - parseFloat(lastDayValue);
        else
            finalValue = value;

        devConfig.previousDayTotalizerValue = lastDayValue;

        if (devConfig.calibration && devConfig.calibration[sensor]) {
            let m = devConfig.calibration[sensor].m;
            let c = devConfig.calibration[sensor].c;
            finalValue = finalValue * m + c;
            console.log(`${sensor} calibrated***`);
        }
        return finalValue;
    } catch (error) {
        return Promise.reject(error);
    }
}
 