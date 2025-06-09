
Configuration Item name	Description	Who to provide
Log a demand to have the request on Nedvana PI Planning	Please follow the process below to get onto the Nedvana PI Planning:
 
•	Digizone SCTASK/REQ number for the demand logged
•	Provide the name of the assigned CSE
•	 
o	CSE to provide the ION Ref number
o	CSE to advise the Priority Number
•	Forward the approved Low-level Design
 
Should you not have the above information, please can you log your request to the Environment Utility using the link below:
 
I&O New Initiative Requests - Service Portal (nedcloud.co.za)
 
To locate the Environment Utility option, once you choose the Priority Drivers, the drop down for I&O Area(s) Involved will appear showing the 3 Utilities for you to choose from – see screenshots below:
Open image-2024-7-5_15-3-22-1.png
 
Open image-2024-7-5_15-3-39-1.png
 
 
Patience Ndhlovu is the Scrum Master for Nedvana Immigration and can assist should you require any guidance.	Project
Application Design:	An application design is required explaining how the solution will integrate with Azure with the relevant vendor specific configurations.	Project
 
 
 
Config Item name	Config Items Details	Description	Who to provide
Name for the SSO Enterprise Application Name:	ECM_Composition	The name that will be used to register the application in Azure.	Project
Identifier (Entity ID):	 https://dev-exstream.nednet.co.za/otdsws/otdstenant/login
The default identifier will be the audience of the SAML response for IDP-initiated SSO.	Vendor
Reply URL (Assertion Consumer Service URL):	 http://dev-exstream.nednet.co.za/design/index.html?tenant=dev-exstream	The default reply URL will be the destination in the SAML response for IDP-initiated SSO.	Vendor
Sign on URL:	https://dev-exstream.nednet.co.za/design/dev-exstream/login	When a user opens this URL, the service provider redirects to Azure AD to authenticate and sign on the user. Azure AD uses the URL to start the application from Microsoft 365 or Azure AD My Apps. When blank, Azure AD does an IdP-initiated sign-on when a user launches the application from Microsoft 365, Azure AD My Apps, or the Azure AD SSO URL. 	Vendor
Relay State (Optional):	 http://dev-exstream.nednet.co.za/design/index.html?tenant=dev-exstream	Specifies to the application where to redirect the user after authentication is completed. Typically the value is a valid URL for the application. However, some applications use this field differently. For more information, ask the application vendor.	Vendor
Logout URL (Optional):	 https://dev-exstream.nednet.co.za/otdsws/otdstenant/dev-exstream/login	Used to send the SAML Logout responses back to the application	Vendor
Application (Client) ID:	 	Application (Client) ID is a unique identifier of the application in Azure.	Information Security
Claims:	 No additional claims	When a user authenticates to the application, Azure AD issues the application a SAML token with information (or claims) about the user that uniquely identifies them. By default, this information includes the user's username, email address, first name, and last name. You might need to customize these claims if, for example, the application requires specific claim values or a Name format other than username. The following claims are included, let me know whether I need to add any further claims:
•	givenname - user.givenname
•	surname - user.surname
•	emailaddress - user.mail
•	Name - user.userprincipalname (email address)
•	Unique User Identifier - user.userprincipalname  (email address)
 	Project/Vendor
Domain group name(s) to assign access to the SSO application:	 <<Nedbank - user Group ECM >	 Please provide the group names that need to be assigned access to this application in Azure.	Project
Automatic Provisioning Required (SCIM) (Optional):	Yes	Please indicate whether your application will require SCIM for auto provisioning of users and groups to your application.	Project/Vendor
SCIM settings:	 Tenant URL: https://dev-exstream.nednet.co.za/otds/otdsws/scim/SCIM-partition/
Secret Token: Ja3njU1Bn23FSheH82Q1FitB6b2Enp4h	Please provide the following:
Tenant URL: https://dev-exstream.nednet.co.za/otds/otdsws/scim/SCIM-partition/
Secret Token: Ja3njU1Bn23FSheH82Q1FitB6b2Enp4h	Project
Environment:	DEV/ETE/QA/PROD	Please indicate which environments you intend implementing SSO in Azure for, i.e. ETE, DEV, QA, Prod.	Project
Application Owner Name:	Andre Sankar	The name of the application owner.	Project
Metadata xml file:	 	Once the SSO Enterprise application has been configured, the metadata xml file will be provided to configure SSO on the application side.	Information Security
Vendor specific configuration steps/guidelines required for integration with Azure:		Please provide any vendor specific configuration guidelines from the Vendor based on their requirements for integration with Azure.	Project/Vendor
Assignment Required:	 	Yes	Information Security
Visible to Users:	 	Yes	Information Security
Infrastructure Changes:	 <<Lizelle to add Change number >>	Once we have all the information we will require an Infrastructure change to be logged for us to implement. Configuration Item (CI): InfoSec Management Information System (Production) and Group name: Information Security F5 Support	Project
 
