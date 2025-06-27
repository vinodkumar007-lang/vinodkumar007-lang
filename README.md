#!/bin/bash

echo "🔧 Running debug-cert.sh to verify OpenText cert..."
/app/debug-cert.sh

echo "✅ Cert verification completed."
echo "🚀 Starting Spring Boot application..."
exec java $JAVA_OPTS_APPEND -jar /deployments/*.jar
