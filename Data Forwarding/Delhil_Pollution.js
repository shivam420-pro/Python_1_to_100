const axios = require('axios');
const forge = require("node-forge");
const moment = require("moment");

/* ---------------- CONFIG ---------------- */

const TOKEN = "MTIwMzIwMjZfbWFnX2Zsb3dfc3lzdGVtX3NfaW5jXzEzMDE1OA==";
const INDUSTRY_ID = "industry_6038";
const STATION_ID = "station_13289"; // may or may not be used by API
const DEVICE_ID = "D00319";

const API_URL = `https://dpcccems.nic.in/dpccb-api/api/industry/${INDUSTRY_ID}/station/${STATION_ID}/data`;

const PUBLIC_KEY_PEM = `-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RH6ZyIj3MXVPjbfirf/
fP/Zqhh1J1EoHPEez/cTfpG5JR0STcTbdLDqBLwZ5ru3eHfVd38e+y+IxYpmYVt9
nq8OVrTArfU6sa9Q5NeAQBcNaDFf2rlRh0Dcxl+YWqfSM5CdZAJ8xLxbw7BmI2ZD
6MXHInNBJHKSFxM/R4FK4Zg8ymH7K8/k69lg+UCT16HBJ00qgwAd9PBcz6XUtFGf
FsFOwoIgt1MOVl9pwU5a5M5oM0TAk15aMiCGJ7en7jEFMm52l0WklthBtMF3Db60
0aDC1EwAX9cbMmKk7f8UkkVpKbW1Kec8edqwb6n00PUg2o86DyeoAysiQAji3Hfr
YwIDAQAB
-----END PUBLIC KEY-----`;

/* ---------------- SIGNATURE ---------------- */
function generateSignature(token) {
    const timestamp = moment().utcOffset('+05:30').format("YYYY-MM-DD HH:mm:ss");
    const message = `${token}$*${timestamp}`;

    const buffer = forge.util.createBuffer(message, 'utf8').getBytes();
    const publicKey = forge.pki.publicKeyFromPem(PUBLIC_KEY_PEM);

    const encrypted = publicKey.encrypt(buffer, "RSA-OAEP", {
        md: forge.md.sha256.create(),
        mgf1: { md: forge.md.sha256.create() }
    });

    return forge.util.encode64(encrypted);
}

/* ---------------- ENCRYPT ---------------- */
function encryptPayload(payload, token) {
    const md = forge.md.sha256.create();
    md.update(token, "utf8");
    const key = md.digest().bytes();

    const cipher = forge.cipher.createCipher("AES-ECB", key);
    cipher.start();
    cipher.update(forge.util.createBuffer(JSON.stringify(payload), "utf8"));
    cipher.finish();

    return forge.util.encode64(cipher.output.bytes());
}

/* ---------------- MAIN FUNCTION ---------------- */
async function sendData() {
    try {
        // ✅ Epoch timestamp (IMPORTANT)
        const timestamp = moment().valueOf();

        // ✅ CORRECT PAYLOAD FORMAT
        const payload = [
            {
                deviceId: DEVICE_ID,
                params: [
                    {
                        unit: "m3/hr",
                        flag: "U",
                        parameter: "Flow",      // MUST match DPCC config
                        value: 3.459,           // your dynamic value
                        timestamp: String(timestamp)
                    }
                ]
            }
        ];

        console.log("\n-------------------------------");
        console.log("📤 RAW PAYLOAD:", JSON.stringify(payload, null, 2));

        // 🔐 Encrypt payload
        const encrypted = encryptPayload(payload, TOKEN);

        // 🔑 Generate signature
        const signature = generateSignature(TOKEN);

        const body = {
            data: encrypted
        };

        const response = await axios.post(API_URL, body, {
            headers: {
                Authorization: `Basic ${TOKEN}`,
                "X-Device-Id": DEVICE_ID,
                signature: signature,
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            timeout: 10000,
            validateStatus: () => true
        });

        console.log("\n HTTP STATUS:", response.status);
        console.log(" CONTENT-TYPE:", response.headers['content-type']);

        // ✅ Handle JSON response
        if (response.headers['content-type']?.includes('application/json')) {

            const res = response.data;
            console.log("📥 API RESPONSE:", res);

            if (res.status === 1) {
                console.log("✅ SUCCESS: Data uploaded successfully");
            } else {
                console.log("❌ FAILED:", res.message || "Unknown error");

                switch (res.status) {
                    case 10:
                        console.log("⚠️ Invalid Token");
                        break;
                    case 101:
                        console.log("⚠️ Invalid Industry ID");
                        break;
                    case 102:
                        console.log("⚠️ Invalid Station ID");
                        break;
                    case 108:
                        console.log("⚠️ Invalid Device ID");
                        break;
                }
            }

        } else {
            console.log("❌ ERROR: Received HTML instead of JSON");
            console.log("👉 Possible reasons:");
            console.log("   - IP not whitelisted");
            console.log("   - Wrong API URL");
            console.log("   - Server blocking request");
        }

    } catch (error) {
        console.error("❌ REQUEST ERROR:", error.message);
    }
}

/* ---------------- AUTO RUN EVERY 30 SEC ---------------- */

let isRunning = false;

// Run once immediately
sendData();

// Run every 30 seconds
setInterval(async () => {
    if (isRunning) return;

    isRunning = true;
    console.log("\n⏱ Sending data every 30 seconds...");
    await sendData();
    isRunning = false;

}, 30000);