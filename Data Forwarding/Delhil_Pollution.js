const axios = require("axios");
const fs = require("fs");
const path = require("path");

/* ---------------- CONFIG ---------------- */

const TOKEN = "MTIwMzIwMjZfbWFnX2Zsb3dfc3lzdGVtX3NfaW5jXzEzMDE1OA==";

const API_URL = `https://dpcccems.nic.in/dlcpcb-api/api/industry/6038/station/Station_13289/data`;

const LOG_FILE = path.join(__dirname, "success_log.json");

/* ---------------- HELPER: READ LOG ---------------- */

function readLog() {
    if (!fs.existsSync(LOG_FILE)) return [];
    try {
        return JSON.parse(fs.readFileSync(LOG_FILE, "utf8"));
    } catch {
        return [];
    }
}

/* ---------------- HELPER: WRITE LOG ---------------- */

function writeLog(logs) {
    fs.writeFileSync(LOG_FILE, JSON.stringify(logs, null, 2));
}

/* ---------------- SAVE SUCCESS ---------------- */

function saveSuccess(timestamp) {
    const logs = readLog();

    logs.push({
        time: timestamp,
        readable: new Date(timestamp).toISOString()
    });

    // keep only last 100 logs (avoid file growth)
    if (logs.length > 100) logs.shift();

    writeLog(logs);
}

/* ---------------- CHECK LAST SUCCESS ---------------- */

function getLastSuccess() {
    const logs = readLog();
    if (logs.length === 0) return null;
    return logs[logs.length - 1].time;
}

/* ---------------- SEND DATA ---------------- */

async function sendData(reason = "normal") {
    try {
        const timestamp = Date.now();

        const payload = [
            {
                deviceId: "MG2511FM_E1",
                params: [
                    {
                        parameter: "Flow",
                        unit: "m3/hr",
                        flag: "U",
                        value: String(3.459),
                        timestamp: timestamp
                    }
                ]
            }
        ];

        console.log(`\n📤 [${reason}] Sending Data...`);

        const response = await axios.post(
            API_URL,
            payload,
            {
                headers: {
                    Authorization: `Basic ${TOKEN}`,
                    "Content-Type": "application/json",
                    Accept: "application/json"
                },
                timeout: 15000,
                validateStatus: () => true
            }
        );

        console.log("🔹 STATUS:", response.status);
        console.log("📥 RESPONSE:", response.data);

        if (response.data?.status === 1) {
            console.log("✅ SUCCESS SAVED");
            saveSuccess(timestamp);
        } else {
            console.log("❌ FAILED");
        }

    } catch (err) {
        console.error("❌ ERROR:", err.message);
    }
}

/* ---------------- MAIN LOOP ---------------- */

async function monitor() {
    const now = Date.now();

    const lastSuccess = getLastSuccess();

    if (!lastSuccess) {
        console.log("⚠️ No previous success → sending data");
        await sendData("first-time");
        return;
    }

    const diffMinutes = (now - lastSuccess) / (1000 * 60);

    console.log(`⏱ Last success ${diffMinutes.toFixed(2)} min ago`);

    if (diffMinutes > 15) {
        console.log("🚨 No success for 15 min → RESENDING DATA");
        await sendData("retry-15min");
    } else {
        console.log("✅ Recent success exists");
        await sendData("normal");
    }
}

/* ---------------- RUN EVERY 1 MIN ---------------- */

console.log("🚀 Service Started (runs every 1 min)");

setInterval(monitor, 60 * 1000);

// run immediately also
monitor();