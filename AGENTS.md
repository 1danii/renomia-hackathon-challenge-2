# Agent Rules (main.py)

Edit implementation only inside `solve()` after `# TODO: Implement your solution here`, NEVER delete the comment.
Keep metric-tracking pieces unchanged (`GeminiTracker`, `/metrics`, `/metrics/reset`).
Preserve API contract for `POST /solve` and return JSON as specified in README.md
If OCR clearly refers to vehicle insurance (`pojištění vozidel`, `vozidlo`, `VIN`, `registrační značka`, `osobní automobil`), set `assetType` to `vehicle`.
If the contract is `na dobu neurčitou` and there is no explicit termination-at-period-end wording, set `actionOnInsurancePeriodTermination` to `auto-renewal`.
