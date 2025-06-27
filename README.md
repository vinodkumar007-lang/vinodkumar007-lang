RUN keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias issuing -file /tmp/certs/Nedbank_Issuing_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias policy -file /tmp/certs/Nedbank_Policy_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias root -file /tmp/certs/Nedbank_Root_Sha2.crt -noprompt \
	&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteissuing -file /tmp/certs/NedETE_Issuing_Sha2 1.cer -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias etepolicy -file /tmp/certs/NedETE_Policy_Sha2 1.cer -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteroot -file /tmp/certs/NedETE_Root_Sha2 1.cer -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias opentext -file /tmp/certs/opentext.crt -noprompt \
    && rm -rf /tmp/certs

    Use "keytool -?, -h, or --help" for this help message
The command '/bin/sh -c keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias issuing -file /tmp/certs/Nedbank_Issuing_Sha2.crt -noprompt     && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias policy -file /tmp/certs/Nedbank_Policy_Sha2.crt -noprompt     && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias root -file /tmp/certs/Nedbank_Root_Sha2.crt -noprompt 	&& keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteissuing -file /tmp/certs/NedETE_Issuing_Sha2 1.cer -noprompt     && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias etepolicy -file /tmp/certs/NedETE_Policy_Sha2 1.cer -noprompt     && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteroot -file /tmp/certs/NedETE_Root_Sha2 1.cer -noprompt     && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias opentext -file /tmp/certs/opentext.crt -noprompt     && rm -rf /tmp/certs' returned a non-zero code: 1

