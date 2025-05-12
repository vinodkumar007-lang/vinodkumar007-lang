C:\Users\CC437236>keytool -import -alias vault-cert -file C:/sers/CC437236/jdk-17.0.12_windows-x64_bin/jdk-17.0.12/lib/security/New folder/hashicorp.crt -keystore C:/sers/CC437236/jdk-17.0.12_windows-x64_bin/jdk-17.0.12/lib/security/New folder/cacerts -storepass changeit
Illegal option:  folder/hashicorp.crt
keytool -importcert [OPTION]...

Imports a certificate or a certificate chain

Options:

 -noprompt               do not prompt
 -trustcacerts           trust certificates from cacerts
 -protected              password through protected mechanism
 -alias <alias>          alias name of the entry to process
 -file <file>            input file name
 -keypass <arg>          key password
 -keystore <keystore>    keystore name
 -cacerts                access the cacerts keystore
 -storepass <arg>        keystore password
 -storetype <type>       keystore type
 -providername <name>    provider name
 -addprovider <name>     add security provider by name (e.g. SunPKCS11)
   [-providerarg <arg>]    configure argument for -addprovider
 -providerclass <class>  add security provider by fully-qualified class name
   [-providerarg <arg>]    configure argument for -providerclass
 -providerpath <list>    provider classpath
 -v                      verbose output

Use "keytool -?, -h, or --help" for this help message
