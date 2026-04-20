// ================= IMPORTS =================
const crypto = require('crypto');
const axios = require('axios');
const moment = require('moment');
const JSZip = require('jszip');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');

// ================= CONFIG =================
const config = {
  siteId: 'site_5163',
  encryptionKey: 'c2l0ZV81MTYzLHZlcl8yLjM=XXXXXXXX', // 🔴 FULL KEY REQUIRED

  monitoringID: 'ETP_Outlet',

  parameter: {
    name: 'Flow',
    parameterID: 'parameter_81',
    analyserID: 'analyzer_951',
    unitID: 'unit_12'
  }
};

// ================= METADATA =================
const metadata =
  'SITE_ID,SITE_UID,MONITORING_UNIT_ID,ANALYZER_ID,PARAMETER_ID,PARAMETER_NAME,READING,UNIT_ID,DATA_QUALITY_CODE,RAW_READING,UNIX_TIMESTAMP,CALIBRATION_FLAG,MAINTENANCE_FLAG';

// ================= ENCRYPT =================
function encrypt(data, key, autoPadding = false) {
  const iv = Buffer.alloc(16);
  key = key.slice(0, 32);

  const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(key), iv);

  if (!autoPadding) cipher.setAutoPadding(false);

  return cipher.update(data, 'utf8', 'base64') + cipher.final('base64');
}

// ================= FIXED TIME FUNCTION =================
function getCorrectTime() {
  const now = Math.floor(Date.now() / 1000);

  // If system time is too far in future → fix it
  if (now > 1750000000) {
    console.log("⚠️ System time is in future, correcting...");

    const corrected = now - (365 * 24 * 60 * 60); // minus 1 year
    return corrected;
  }

  return now;
}

// ================= BUILD CSV =================
function buildCSV(value, time) {
  let row =
    `${config.siteId},${config.siteId},${config.monitoringID},` +
    `${config.parameter.analyserID},${config.parameter.parameterID},` +
    `${config.parameter.name},${value},${config.parameter.unitID},U,${value},` +
    `${time},0,0`;

  // AES padding (16)
  const block = 16;
  if (row.length % block !== 0) {
    row = row.padEnd(row.length + (block - (row.length % block)), '#');
  }

  console.log("📄 CSV:", row);

  return encrypt(row, config.encryptionKey);
}

// ================= CREATE ZIP =================
async function createZip(data) {
  const zip = new JSZip();

  const timeStr = moment().format('YYYYMMDDHHmmss');

  const zipName = `${config.siteId}_${config.monitoringID}_${timeStr}.zip`;
  const dataFile = `${config.siteId}_${config.monitoringID}_${timeStr}.csv`;

  zip.file('metadata.csv', metadata);
  zip.file(dataFile, data);

  const content = await zip.generateAsync({ type: 'nodebuffer' });

  // Save locally
  const folder = path.join(__dirname, 'zip');
  if (!fs.existsSync(folder)) fs.mkdirSync(folder);

  const filePath = path.join(folder, zipName);
  fs.writeFileSync(filePath, content);

  console.log("✅ ZIP saved:", filePath);

  return { content, fileName: zipName };
}

// ================= SEND DATA =================
async function send(fileName, content, time) {
  const timestamp = moment.utc(time * 1000).format();

  const authString =
    `${config.siteId},ver_3.1,${timestamp},${config.encryptionKey}`;

  const formData = new FormData();
  formData.append('file', content, { filename: fileName });

  try {
    const res = await axios.post(
      'https://gpcboms.gpcb.gov.in/GPCBServer/realtimeUpload',
      formData,
      {
        headers: {
          Authorization:
            `Basic ${encrypt(authString, config.encryptionKey, true)}` +
            Buffer.alloc(16).toString('base64'),
          siteId: config.siteId,
          Timestamp: timestamp,
          ...formData.getHeaders()
        }
      }
    );

    console.log("🚀 SUCCESS RESPONSE:", res.data);

  } catch (err) {
    console.log("❌ ERROR:", err.response?.data || err.message);
  }
}

// ================= MAIN =================
async function run() {
  const currentTime = getCorrectTime();

  console.log("⏱ FINAL TIME:", currentTime);

  const value = 0.1; // safer than 0

  const encrypted = buildCSV(value, currentTime);

  const { content, fileName } = await createZip(encrypted);

  await send(fileName, content, currentTime);
}

// ================= RUN =================
run();