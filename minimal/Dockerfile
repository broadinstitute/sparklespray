FROM alpine:3.3
COPY get-pip.py /
RUN apk add --no-cache python && \
   python get-pip.py && \
   pip install boto

