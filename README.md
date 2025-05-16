"timestamp":"2025-05-12T09:06:07.843956600+02:00"[Africa/Johannesburg]
This is invalid JSON — the [Africa/Johannesburg] timezone suffix is not quoted or handled properly, and Jackson rightly fails with:

Unexpected character ('[' (code 91)): was expecting comma to separate Object entries
