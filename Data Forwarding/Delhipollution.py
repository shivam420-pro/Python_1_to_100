import requests
import json
import time
import base64
from datetime import datetime
from hashlib import sha256
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

# ---------------- CONFIG ---------------- #

TOKEN = "MTIwMzIwMjZfbWFnX2Zsb3dfc3lzdGVtX3NfaW5jXzEzMDE1OA=="
INDUSTRY_ID = "industry_6038"
STATION_ID = "station_13289"
DEVICE_ID = "D00319"

API_URL = f"https://dpcccems.nic.in/dpccb-api/api/industry/{INDUSTRY_ID}/station/{STATION_ID}/data"

PUBLIC_KEY_PEM = b"""-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0RH6ZyIj3MXVPjbfirf/
fP/Zqhh1J1EoHPEez/cTfpG5JR0STcTbdLDqBLwZ5ru3eHfVd38e+y+IxYpmYVt9
nq8OVrTArfU6sa9Q5NeAQBcNaDFf2rlRh0Dcxl+YWqfSM5CdZAJ8xLxbw7BmI2ZD
6MXHInNBJHKSFxM/R4FK4Zg8ymH7K8/k69lg+UCT16HBJ00qgwAd9PBcz6XUtFGf
FsFOwoIgt1MOVl9pwU5a5M5oM0TAk15aMiCGJ7en7jEFMm52l0WklthBtMF3Db60
0aDC1EwAX9cbMmKk7f8UkkVpKbW1Kec8edqwb6n00PUg2o86DyeoAysiQAji3Hfr
YwIDAQAB
-----END PUBLIC KEY-----"""

# ---------------- SIGNATURE ---------------- #

def generate_signature(token):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = f"{token}$*{timestamp}".encode()

    public_key = serialization.load_pem_public_key(PUBLIC_KEY_PEM)

    encrypted = public_key.encrypt(
        message,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )

    return base64.b64encode(encrypted).decode()

# ---------------- ENCRYPT ---------------- #

def encrypt_payload(payload, token):
    key = sha256(token.encode()).digest()

    cipher = AES.new(key, AES.MODE_ECB)
    encrypted = cipher.encrypt(pad(json.dumps(payload).encode(), AES.block_size))

    return base64.b64encode(encrypted).decode()

# ---------------- MAIN FUNCTION ---------------- #

def send_data():
    try:
        # ✅ Epoch timestamp (ms)
        timestamp = str(int(time.time() * 1000))

        payload = [
            {
                "deviceId": DEVICE_ID,
                "params": [
                    {
                        "unit": "m3/hr",
                        "flag": "U",
                        "parameter": "Flow",
                        "value": 3.459,
                        "timestamp": timestamp
                    }
                ]
            }
        ]

        print("\n-------------------------------")
        print("📤 RAW PAYLOAD:", json.dumps(payload, indent=2))

        encrypted_data = encrypt_payload(payload, TOKEN)
        signature = generate_signature(TOKEN)

        body = {
            "data": encrypted_data
        }

        headers = {
            "Authorization": f"Basic {TOKEN}",
            "X-Device-Id": DEVICE_ID,
            "signature": signature,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.post(API_URL, json=body, headers=headers, timeout=10)

        print("\n🔍 HTTP STATUS:", response.status_code)
        print("🔍 CONTENT-TYPE:", response.headers.get("Content-Type"))

        if "application/json" in response.headers.get("Content-Type", ""):
            res = response.json()
            print("📥 API RESPONSE:", res)

            if res.get("status") == 1:
                print("✅ SUCCESS: Data uploaded successfully")
            else:
                print("❌ FAILED:", res.get("message"))

        else:
            print("❌ ERROR: Received HTML instead of JSON")
            print("👉 Possible reasons:")
            print("   - IP not whitelisted")
            print("   - Wrong API URL")
            print("   - Server blocking request")

    except Exception as e:
        print("❌ REQUEST ERROR:", str(e))

# ---------------- AUTO RUN EVERY 30 SEC ---------------- #

if __name__ == "__main__":
    send_data()

    while True:
        print("\n⏱ Sending data every 30 seconds...")
        send_data()
        time.sleep(30)