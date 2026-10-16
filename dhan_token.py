from dotenv import load_dotenv
load_dotenv()

# ============================================================
# DHAN ACCESS TOKEN
# ============================================================

DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzkyMjA2NjExLCJpYXQiOjE3OTIxMjAyMTEsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTE0MDU2MDg5In0.Hpixpig5cTnX2me5YDbneyy-cZksB1SyxoQvRE0UnEOVJtZK24yEo33HkMCtcw1dmkJuwLYP3UFtyk-Z3eISHQ"


# ============================================================
# GET DHAN ACCESS TOKEN
# ============================================================

def get_access_token():
    return DHAN_ACCESS_TOKEN

 