const axios = require("axios");
const forge = require("node-forge");
const moment = require("moment");

/* ---------------- CONFIG ---------------- */

const TOKEN = "MTIwMzIwMjZfbWFnX2Zsb3dfc3lzdGVtX3NfaW5jXzEzMDE1OA==";
const INDUSTRY_ID = "industry_6038";
const STATION_ID = "station_13289";
const DEVICE_ID = "device_12478";

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
    const timestamp = moment().format("YYYY-MM-DD HH:mm:ss");
    const message = `${token}$*${timestamp}`;

    const publicKey = forge.pki.publicKeyFromPem(PUBLIC_KEY_PEM);

    const encrypted = publicKey.encrypt(
        forge.util.encodeUtf8(message),
        "RSA-OAEP",
        {
            md: forge.md.sha256.create(),
            mgf1: { md: forge.md.sha256.create() }
        }
    );

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

/* ---------------- MAIN ---------------- */
async function sendData() {
    try {
        const timestamp = Date.now(); // epoch ms

        const payload = [
            {
                deviceId: DEVICE_ID,
                params: [
                    {
                        unit: "m3/hr",
                        flag: "U",
                        parameter: "Flow",
                        value: 3.459,
                        timestamp: String(timestamp)
                    }
                ]
            }
        ];

        console.log("\n📤 PAYLOAD:", JSON.stringify(payload, null, 2));

        const encrypted = encryptPayload(payload, TOKEN);
        const signature = generateSignature(TOKEN);

        const response = await axios.post(
            API_URL,
            { data: encrypted },
            {
                headers: {
                    Authorization: `Basic ${TOKEN}`,
                    "X-Device-Id": DEVICE_ID,
                    "Signature": signature,   // ✅ FIXED
                    "Content-Type": "application/json",
                    Accept: "application/json"
                },
                timeout: 15000,
                validateStatus: () => true
            }
        );

        console.log("\n🔹 STATUS:", response.status);
        console.log("🔹 CONTENT-TYPE:", response.headers["content-type"]);

        // ✅ DEBUG RAW RESPONSE
        console.log("\n📥 RAW RESPONSE:");
        console.log(response.data);

        if (response.headers["content-type"]?.includes("application/json")) {
            if (response.data.status === 1) {
                console.log("✅ SUCCESS");
            } else {
                console.log("❌ FAILED:", response.data.message);
            }
        } else {
            console.log("\n❌ HTML RESPONSE DETECTED");

            if (typeof response.data === "string") {
                if (response.data.includes("Access Denied")) {
                    console.log("🚨 IP NOT WHITELISTED");
                } else if (response.data.includes("404")) {
                    console.log("🚨 WRONG API URL");
                } else {
                    console.log("🚨 SERVER BLOCKING / UNKNOWN ISSUE");
                }
            }
        }

    } catch (err) {
        console.error("❌ ERROR:", err.message);
    }
}

/* ---------------- RUN ---------------- */

sendData();