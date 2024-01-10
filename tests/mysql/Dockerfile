ARG IMAGE
FROM ${IMAGE}

# Copy SSL certificate (and key)
COPY certs/cert.crt /etc/mysql/ssl/server.crt
COPY certs/ca.crt /etc/mysql/ssl/ca.crt
COPY certs/cert.key /etc/mysql/ssl/server.key
COPY my.cnf /etc/mysql/my.cnf

# Fix permissions
RUN chown mysql:mysql /etc/mysql/ssl/server.crt /etc/mysql/ssl/server.key
RUN chmod 0600 /etc/mysql/ssl/server.crt /etc/mysql/ssl/server.key
