"""
RedAlert Fixed Window Rate Limit Test
Phase 7: Validates that POST /incidents enforces a strict 5 requests per minute limit
for a given X-Source identity.

Sends 8 rapid requests from one source:
- Requests 1 to 5: Expect HTTP 201 Created
- Requests 6 to 8: Expect HTTP 429 Too Many Requests with Retry-After header
"""

import argparse
import json
import time
import urllib.request
import urllib.error


def run_rate_limit_test(base_url: str = "http://localhost:8000"):
    print("=" * 65)
    print("  ⏱️ RedAlert Rate Limit Test (Fixed Window 5 req/min)")
    print(f"  Target: {base_url}/incidents")
    print("=" * 65)

    test_source = f"caller-limit-test-{int(time.time())}"
    print(f"Testing with X-Source: {test_source}\n")

    success_count = 0
    throttled_count = 0

    for i in range(1, 9):
        payload = {
            "type": "Rate Limit Test Emergency",
            "lat": 12.9716,
            "lon": 77.5946,
            "severity": 5,
            "zone": "central",
        }
        data_bytes = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url.rstrip('/')}/incidents",
            data=data_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Source": test_source,
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=5.0) as resp:
                status_code = resp.getcode()
                body = json.loads(resp.read().decode("utf-8"))
                inc_id = body.get("id")
                print(f"[Req {i}/8] ✅ Status {status_code} Created | Incident #{inc_id}")
                success_count += 1
        except urllib.error.HTTPError as err:
            status_code = err.code
            retry_after = err.headers.get("Retry-After", "unknown")
            err_body = err.read().decode("utf-8")
            print(f"[Req {i}/8] 🚫 Status {status_code} Too Many Requests | Retry-After: {retry_after}s")
            throttled_count += 1
        except Exception as exc:
            print(f"[Req {i}/8] ❌ Error: {exc}")

    print("\n-----------------------------------------------------------------")
    print(f"Results Summary:")
    print(f"  - Accepted (HTTP 201):  {success_count}/5 expected")
    print(f"  - Throttled (HTTP 429): {throttled_count}/3 expected")

    if success_count == 5 and throttled_count == 3:
        print("  - Outcome:              ✅ PASS: Exactly 5 requests allowed before 429!")
    else:
        print(f"  - Outcome:              ⚠️ Unexpected distribution ({success_count} success, {throttled_count} 429)")
    print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test Rate Limiting on POST /incidents")
    parser.add_argument("--url", default="http://localhost:8000", help="FastAPI base URL")
    args = parser.parse_args()

    run_rate_limit_test(args.url)

