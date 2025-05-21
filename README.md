PS C:\Users\CC437236> az login
The command failed with an unexpected error. Here is the traceback:
HTTPSConnectionPool(host='login.microsoftonline.com', port=443): Max retries exceeded with url: /organizations/v2.0/.well-known/openid-configuration (Caused by ConnectTimeoutError(<urllib3.connection.HTTPSConnection object at 0x03FC0370>, 'Connection to login.microsoftonline.com timed out. (connect timeout=None)'))
Traceback (most recent call last):
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connection.py", line 174, in _new_conn
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/util/connection.py", line 96, in create_connection
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/util/connection.py", line 86, in create_connection
TimeoutError: [WinError 10060] A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connectionpool.py", line 699, in urlopen
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connectionpool.py", line 382, in _make_request
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connectionpool.py", line 1010, in _validate_conn
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connection.py", line 358, in connect
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connection.py", line 179, in _new_conn
urllib3.exceptions.ConnectTimeoutError: (<urllib3.connection.HTTPSConnection object at 0x03FC0370>, 'Connection to login.microsoftonline.com timed out. (connect timeout=None)')

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\requests/adapters.py", line 439, in send
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connectionpool.py", line 783, in urlopen
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/connectionpool.py", line 755, in urlopen
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\urllib3/util/retry.py", line 574, in increment
urllib3.exceptions.MaxRetryError: HTTPSConnectionPool(host='login.microsoftonline.com', port=443): Max retries exceeded with url: /organizations/v2.0/.well-known/openid-configuration (Caused by ConnectTimeoutError(<urllib3.connection.HTTPSConnection object at 0x03FC0370>, 'Connection to login.microsoftonline.com timed out. (connect timeout=None)'))

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\knack/cli.py", line 233, in invoke
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/commands/__init__.py", line 663, in execute
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/commands/__init__.py", line 726, in _run_jobs_serially
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/commands/__init__.py", line 697, in _run_job
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/commands/__init__.py", line 333, in __call__
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/commands/command_operation.py", line 121, in handler
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/command_modules/profile/custom.py", line 139, in login
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/_profile.py", line 154, in login
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/auth/identity.py", line 153, in login_with_auth_code
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\azure/cli/core/auth/identity.py", line 112, in _msal_app
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\msal/application.py", line 1685, in __init__
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\msal/application.py", line 533, in __init__  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\msal/authority.py", line 120, in __init__
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\msal/authority.py", line 175, in tenant_discovery
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\msal/individual_cache.py", line 269, in wrapper
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\requests/sessions.py", line 555, in get
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\requests/sessions.py", line 542, in request  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\requests/sessions.py", line 655, in send
  File "D:\a\_work\1\s\build_scripts\windows\artifacts\cli\Lib\site-packages\requests/adapters.py", line 504, in send
requests.exceptions.ConnectTimeout: HTTPSConnectionPool(host='login.microsoftonline.com', port=443): Max retries exceeded with url: /organizations/v2.0/.well-known/openid-configuration (Caused by ConnectTimeoutError(<urllib3.connection.HTTPSConnection object at 0x03FC0370>, 'Connection to login.microsoftonline.com timed out. (connect timeout=None)'))
To check existing issues, please visit: https://github.com/Azure/azure-cli/issues
To open a new issue, please run `az feedback`
