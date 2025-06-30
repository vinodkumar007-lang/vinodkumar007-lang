FROM redhat.ncr.devops.nednet.co.za/ubi8/openjdk-17-runtime@sha256:763507bf338323e15bfc1e8913bb1daa76c724642d0c4e65c2c7682dc87df144

USER root
WORKDIR /app

# Set timezone and JVM options
ENV TZ=Africa/Johannesburg
ENV JAVA_OPTS_APPEND="-Duser.timezone=Africa/Johannesburg -Duser.language=en -Duser.country=ZA -Xms256m -Xmx512m"
ENV KEYSTORE_PASS=changeit

# Set system timezone
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Copy certs to temporary location
COPY Docker/certs /tmp/certs

# Import all required certs into Java truststore
RUN keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias issuing -file /tmp/certs/Nedbank_Issuing_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias policy -file /tmp/certs/Nedbank_Policy_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias root -file /tmp/certs/Nedbank_Root_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteissuing -file "/tmp/certs/NedETE_Issuing_Sha2 1.cer" -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias etepolicy -file "/tmp/certs/NedETE_Policy_Sha2 1.cer" -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias eteroot -file "/tmp/certs/NedETE_Root_Sha2 1.cer" -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias opentext -file /tmp/certs/opentext.crt -noprompt \
    && rm -rf /tmp/certs

# Copy debug and entrypoint scripts
COPY Docker/debug-cert.sh /app/debug-cert.sh
COPY Docker/entrypoint.sh /app/entrypoint.sh

# Set permissions
RUN chmod +x /app/debug-cert.sh /app/entrypoint.sh \
    && chown -R jboss:jboss /app \
    && chown -R jboss:jboss /mnt

USER jboss

# Copy the application JAR
COPY target/*.jar /deployments/

# Set custom entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]
