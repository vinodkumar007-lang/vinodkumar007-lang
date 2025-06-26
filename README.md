COPY Docker/certs /tmp/certs

RUN keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias issuing -file /tmp/certs/Nedbank_Issuing_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias policy -file /tmp/certs/Nedbank_Policy_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias root -file /tmp/certs/Nedbank_Root_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias opentext -file /tmp/certs/opentext.crt -noprompt \
    && rm -rf /tmp/certs
