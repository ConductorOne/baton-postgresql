FROM gcr.io/distroless/static-debian11:nonroot
ENTRYPOINT ["/baton-postgresql"]
COPY baton-postgresql /