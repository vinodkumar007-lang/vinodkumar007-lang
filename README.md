FROM redhat.ncr.devops.nednet.co.za/ubi8/openjdk-17-runtime@sha256:763507bf338323e15bfc1e8913bb1daa76c724642d0c4e65c2c7682dc87df144
USER root
WORKDIR /app
 
# Set the correct time zone
ENV TZ=Africa/Johannesburg
ENV JAVA_OPTS_APPEND="-Duser.timezone=Africa/Johannesburg -Duser.language=en -Duser.country=ZA -Xms256m -Xmx512m"
ENV KEYSTORE_PASS=changeit
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
 
COPY Docker/certs /tmp/certs

RUN keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias issuing -file /tmp/certs/Nedbank_Issuing_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias policy -file /tmp/certs/Nedbank_Policy_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias root -file /tmp/certs/Nedbank_Root_Sha2.crt -noprompt \
    && keytool -trustcacerts -cacerts -storepass $KEYSTORE_PASS -importcert -alias opentext -file /tmp/certs/opentext.crt -noprompt \
    && rm -rf /tmp/certs

RUN chown -R jboss:jboss /app \
   && chown -R jboss:jboss /mnt
 
USER jboss
 
COPY target/*.jar /deployments/
