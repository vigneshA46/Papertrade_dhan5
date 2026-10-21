import base64, json, datetime

token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzkyNjQwNjkzLCJpYXQiOjE3OTI1NTQyOTMsInRva2VuQ29uc3VtZXJUeXBlIjoiQVBQIiwiZGhhbkNsaWVudElkIjoiMTEwNzQyNTI3NSJ9.3aPk28o3kqgXfKKLmTtl0FsH8lX-qy1wHSB79tDXU51TgO9yC8cEJfJTlzjxYImzBsuk2zQ6gsLTQD3TxYvt9A"
payload = token.split(".")[1]
payload += "=" * (-len(payload) % 4)
info = json.loads(base64.urlsafe_b64decode(payload))

print("TOKEN CLIENT :", info.get("dhanClientId"))
print("ISSUED       :", datetime.datetime.fromtimestamp(info.get("iat", 0)))
print("EXPIRES      :", datetime.datetime.fromtimestamp(info.get("exp", 0)))
print("ALL FIELDS   :", {k: v for k, v in info.items() if k not in ("dhanClientId",)})