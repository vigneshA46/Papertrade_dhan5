from dotenv import load_dotenv
load_dotenv()

# ============================================================
# DHAN ACCESS TOKEN
# ============================================================

DHAN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzkyNjQ5ODAyLCJpYXQiOjE3OTI1NjM0MDIsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA3NDI1Mjc1In0.-ICeWDsbiu7vFdmRs1mECEP1l1itdOmT1D8G46AwU7tn1SOiOoIiObKB16tIJpFoUG-cjKIMY2ONavlNoG6hLQ"


# ============================================================
# GET DHAN ACCESS TOKEN
# ============================================================

def get_access_token():
    return DHAN_ACCESS_TOKEN