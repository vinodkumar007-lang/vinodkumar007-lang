1.For the field where type is mentioned as object, should we expect any specific fields or structure inside that object, or can it be an empty object?

2.Should timestamp, startTime, and endTime be in  format (YYYY-MM-DDTHH:MM:SSZ)? or system date

3.Please clarify what data represents jobName in the composition application context

4.systemEnv DEV/ETE/QA/PROD/local - no local , setting value based on Environment

5.Should timestamp use system time or value from incoming event?

6. Do we require everytime these error data or only required if in case failed.

Are success, errorCode, and errorMessage required in every file record?

How should we handle retryFlag and retryCount — initialize as false/0 or skip if not retried?


PascalCase vs camelCase
