from dotenv import load_dotenv
load_dotenv()

# ============================================================
# DHAN ACCESS TOKEN
# ============================================================

DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzkxODY1NDEwLCJpYXQiOjE3OTE3NzkwMTAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTE0MDU2MDg5In0.pTLnS6w7NRTUL735pGdRIWj5fdutmSmLBI77Ffn8RyY98-1HaX9hopGVJN6rFHWH0sQ9eT4Iw9iN8Cy1MIB6fw"


# ============================================================
# GET DHAN ACCESS TOKEN
# ============================================================

def get_access_token():
    return DHAN_ACCESS_TOKEN

    