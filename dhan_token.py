from dotenv import load_dotenv
load_dotenv()

# ============================================================
# DHAN ACCESS TOKEN
# ============================================================

DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzkxNjE0OTI4LCJpYXQiOjE3OTE1Mjg1MjgsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTE0MDU2MDg5In0.kVpx6_187WgmEv4tyKTJ3Mr6NgLWn9DOD-8dehGtGD0ODpDDnNb01JaYk-joBtStOSOvUl6CRsgYe6e7a1bp9Q"


# ============================================================
# GET DHAN ACCESS TOKEN
# ============================================================

def get_access_token():
    return DHAN_ACCESS_TOKEN