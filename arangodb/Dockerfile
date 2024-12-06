# Todo se crea desde Centos9
FROM moongeun/centos9

# Establece la versión de ArangoDB que deseas instalar
ENV ARANGODB_VERSION=3.12.3
ENV ARANGODB_HOME=/opt/arangodb

# Instalar dependencias necesarias
RUN dnf -y update && \
    dnf -y install wget tar gcc glibc && \
    dnf clean all

   
# Descargar e instalar ArangoDB
RUN wget https://download.arangodb.com/arangodb312/Community/Linux/arangodb3-3.12.3-1.0.x86_64.rpm -O /tmp/arangodb.rpm && \
    dnf -y localinstall /tmp/arangodb.rpm && \
    rm -f /tmp/arangodb.rpm

# Crear un directorio para los datos de ArangoDB
RUN mkdir -p /var/lib/arangodb3 /var/log/arangodb3 && \
    chown -R root:root /var/lib/arangodb3 /var/log/arangodb3

# Exponer los puertos necesarios (8529 para la interfaz web y API)
EXPOSE 8529

# Configurar ArangoDB para ejecutarse con el usuario predeterminado
CMD ["arangod", "--server.endpoint", "tcp://0.0.0.0:8529", "--log.file", "/var/log/arangodb3/arangod.log"]
