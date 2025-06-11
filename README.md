, which we've traced in logs to an Azure AD authentication issue.

The error message:

AADSTS700016: Application with identifier  client-id ‘[f8b3e641-5baa-4a97-b58d-a7deecc0f5c2]’ was not found in the directory ‘Nedbank’.

This confirms that the application (client ID) is not registered or accessible in the 'Nedbank' Azure AD tenant.

Please confirm the correct Azure AD tenant ID and ensure that the application is registered in that tenant. Once the correct tenant is configured, the authentication 
