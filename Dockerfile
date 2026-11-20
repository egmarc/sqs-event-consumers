FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -o /out/db-worker ./cmd/db-worker && \
    CGO_ENABLED=0 go build -trimpath -o /out/s3-worker ./cmd/s3-worker && \
    CGO_ENABLED=0 go build -trimpath -o /out/reconciler ./cmd/reconciler

FROM public.ecr.aws/lambda/provided:al2023 AS db-lambda
COPY --from=build /out/db-worker /var/runtime/bootstrap
ENTRYPOINT ["/var/runtime/bootstrap"]

FROM public.ecr.aws/lambda/provided:al2023 AS s3-lambda
COPY --from=build /out/s3-worker /var/runtime/bootstrap
ENTRYPOINT ["/var/runtime/bootstrap"]

FROM public.ecr.aws/lambda/provided:al2023 AS reconciler-lambda
COPY --from=build /out/reconciler /var/runtime/bootstrap
ENTRYPOINT ["/var/runtime/bootstrap"]

FROM gcr.io/distroless/static-debian12:nonroot AS local
COPY --from=build /out/db-worker /app/db-worker
COPY --from=build /out/s3-worker /app/s3-worker
COPY --from=build /out/reconciler /app/reconciler
USER nonroot:nonroot
ENTRYPOINT ["/app/db-worker"]
