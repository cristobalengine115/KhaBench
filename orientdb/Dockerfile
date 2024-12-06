# Todo se crea desde Centos9
FROM moongeun/centos9

# Establece la versión de OrientDB que deseas instalar
ENV ORIENTDB_VERSION=3.2.36
ENV ORIENTDB_HOME=/opt/orientdb

# Instalar dependencias necesarias
RUN dnf -y update && \
    dnf -y install java-11-openjdk wget tar && \
    dnf clean all

# Descargar e instalar OrientDB
RUN wget https://repo1.maven.org/maven2/com/orientechnologies/orientdb-community/3.2.36/orientdb-community-3.2.36.tar.gz -O /tmp/orientdb.tar.gz && \
    mkdir -p $ORIENTDB_HOME && \
    tar -xzf /tmp/orientdb.tar.gz -C $ORIENTDB_HOME --strip-components=1 && \
    rm -f /tmp/orientdb.tar.gz

# Cambiar permisos para el usuario OrientDB
RUN useradd -ms /bin/bash orientdb && \
    chown -R orientdb:orientdb $ORIENTDB_HOME

# Agregar OrientDB al PATH
ENV PATH=$ORIENTDB_HOME/bin:$PATH

# Exponer los puertos necesarios (2424 para el servidor y 2480 para la consola web)
EXPOSE 2424 2480

# Cambiar al usuario 'orientdb'
USER orientdb

# Comando predeterminado para iniciar OrientDB
CMD ["server.sh"]
