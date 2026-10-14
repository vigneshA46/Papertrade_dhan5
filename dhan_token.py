from dotenv import load_dotenv
load_dotenv()

# ============================================================
# DHAN ACCESS TOKEN
# ============================================================

DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzkyMDM2OTkwLCJpYXQiOjE3OTE5NTA1OTAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTE0MDU2MDg5In0.1GF5t5S8TJWQHhDus2K6TQpEshA8jfTgC61Ks5bjsO2_mohuoP87jMZD22XSrV_JBm8-bsKo74BRbW9ii3YVEw"


# ============================================================
# GET DHAN ACCESS TOKEN
# ============================================================

def get_access_token():
    return DHAN_ACCESS_TOKEN

    