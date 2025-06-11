Subject: Azure App Deployment – Tenant Configuration Issue

Hi [Client Name],

We’ve successfully deployed the application to Azure and verified the code and configuration. However, we’re currently encountering a 504 Gateway Timeout, which we've traced to an Azure AD authentication issue.

The error message:

AADSTS700016: Application with identifier ‘[client-id]’ was not found in the directory ‘Nedbank’.

This confirms that the application (client ID) is not registered or accessible in the 'Nedbank' Azure AD tenant.

What we need:
Please confirm the correct Azure AD tenant ID and ensure that the application is registered in that tenant. Once the correct tenant is configured, the authentication will succeed, and the application will work as expected.
