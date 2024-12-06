# Todo se crea desde Centos9
FROM moongeun/centos9

# Establecer variables de entorno necesarias
ENV CRATE_VERSION=5.3.1
ENV CRATE_HOME=/opt/crate

# Instalar dependencias necesarias
RUN dnf -y update && \
    dnf -y install java-11-openjdk wget && \
    dnf clean all

# Descargar e instalar CrateDB
RUN wget https://cdn.crate.io/downloads/releases/crate-$CRATE_VERSION.tar.gz -O /tmp/crate.tar.gz && \
    mkdir -p $CRATE_HOME && \
    tar -xzf /tmp/crate.tar.gz -C $CRATE_HOME --strip-components=1 && \
    rm -f /tmp/crate.tar.gz

# Agregar CrateDB al PATH
ENV PATH=$CRATE_HOME/bin:$PATH

# Crear un usuario para ejecutar CrateDB de forma segura
RUN useradd -ms /bin/bash crate && \
    chown -R crate:crate $CRATE_HOME

# Cambiar a usuario 'crate'
USER crate

# Exponer los puertos
EXPOSE 4200 5432

# Comando predeterminado para iniciar CrateDB
CMD ["crate", "start"]