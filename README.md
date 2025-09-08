Subject: Impact of Missing <cisNumber> in std.xml and Required Handling

Dear [Client Name],

As discussed, some source systems do not provide the <cisNumber> field in the std.xml. Please find below the impact analysis and required handling steps:

Impact on Existing Processing

When <cisNumber> tag is missing:

Current code may throw a NullPointerException while parsing.

When <cisNumber> tag is present but empty (<cisNumber></cisNumber>):

Processing continues without error.

In summary.json, customerId will appear as "" (empty string).

Grouping will fall back to accountNumber.

Grouping / Summary Behavior:

Normally, grouping is done on customerId + accountNumber.

If cisNumber is missing/empty, grouping will be done on accountNumber only.

Multiple accounts under the same customer cannot be linked in this case.

Downstream System Impact:

Since summary.json will contain customerId as "" for such cases,
downstream systems must also implement null/empty checks when using customerId.

Code Changes on FileManager Side

Add null checks while reading <cisNumber> to avoid runtime exceptions.

Apply fallback logic to group only by accountNumber when cisNumber is missing/empty.

Ensure summary.json always includes the customerId field (as "" if not provided).
