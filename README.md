Kubernetes Ingress Solution
Do we need to configure Ingress to:
Match /file-manager/... in the external URL
Rewrite it before forwarding to the app (so the app still receives /api/file/process)

apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: file-manager-ingress
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /$2
spec:
  rules:
    - host: <env-url>  # Replace with your environment-specific domain
      http:
        paths:
          - path: /file-manager(/|$)(.*)
            pathType: Prefix
            backend:
              service:
                name: file-manager
                port:
                  number: 9091
