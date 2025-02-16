import machineid
import requests
import json

ACCOUNT_ID = "b1be6412-4212-471a-ae28-dfcb1d65717f"
LICENSE_KEY = "eyJhbGciOiJSUzI1NiJ9.eyJqdGkiOiJhNzU4NDYzYy1iMTExLTQzYjAtODFhMy1jZWUzZmI2MDA3NWUiLCJpc3MiOiJodHRwczovL2tleWdlbi5zaCIsImF1ZCI6ImIxYmU2NDEyLTQyMTItNDcxYS1hZTI4LWRmY2IxZDY1NzE3ZiIsInN1YiI6ImU0NzM5N2VkLTE0NWQtNDUxMS05OWJmLWQ4OTY2ZmFjYjg5YSIsImlhdCI6MTczOTQ3MTI0NSwibmJmIjoxNzM5NDcxMjQ1LCJleHAiOjE3NDE4MjQwMDB9.mqfbrnfazkwmrqysEtT57XZpVVI85x7kKUxo2eLyAOpA4kUcDKIdh11wLcSfPMLkv_O6osC7MKOJMoCyWklRLWzvAnquHx01Hn0AVUYARoGbpnP174adn95_iaSQJLy61Bwr7tNJzmvcy8q6awjwyvnaHVgtLVC1_t967vCfrCmOQJLoSZGmm2lMs1-MXugPHXLsdQqrr-s8T3Md9Fmufojtqy2YtUizd2Ad4Cfdq5o3qusVRdR-V4hOACB976hyvHV0wof3YJgmll1-A1ZHp279SRVX5QHpIcwBmwo9WXB4Jz5dY0FwaU7s1g2ZLnKC8LKH1oxOJiAx2Jc4Ypn7cQ"
fingerprint = machineid.hashed_id()

headers={
    "Content-Type": "application/vnd.api+json",
    "Accept": "application/vnd.api+json"
  }

data=json.dumps({
    "meta": {
      "key": LICENSE_KEY,
      "scope": {
        "fingerprint": fingerprint
      }
    }
  })

res = requests.post(f"https://api.keygen.sh/v1/accounts/{ACCOUNT_ID}/licenses/actions/validate-key", headers=headers, data=data)

if res.status_code == 200:
    print(res.json())
else:
    print(f"Erro: {res.status_code} - {res.text}")