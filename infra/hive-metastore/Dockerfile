# HMS для ДЗ-2: apache/hive:4.0.1 (Hadoop 3.3.6) без внешних драйверов.
# Добавляем postgres-jdbc (backing БД) и hadoop-aws + aws-sdk-bundle,
# чтобы HMS мог создавать/проверять пути s3:// при CREATE DATABASE.
# Конфиг hive-site.xml: -D из SERVICE_OPTS не долетают до hadoop Configuration
# (проверено), поэтому все свойства — в XML.
#
# Jar'ы НЕ хранятся в репозитории (296 МБ — aws-java-sdk-bundle): они
# загружаются с Maven Central на этапе сборки с проверкой sha256 — бит-в-бит
# те же файлы, что прошли spike-1 и рабочий прогон. «Офлайн-сборки» не существует:
# FROM ниже всё равно тянет базовые образы из registry — если сеть доступна
# для них, она доступна и для Maven Central. Повторные сборки берут слой
# из кэша Docker.
#
# Согласование версий (рассинхрон = NoSuchMethodError в рантайме):
#   hadoop-aws 3.3.6 == Hadoop базового образа apache/hive:4.0.1;
#   aws-java-sdk-bundle 1.12.367 — зависимость hadoop-aws 3.3.6.
FROM curlimages/curl:8.11.1 AS jars
WORKDIR /jars
RUN set -eux; \
    curl -fsSL --retry 5 --retry-delay 3 -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar; \
    curl -fsSL --retry 5 --retry-delay 3 -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar; \
    curl -fsSL --retry 5 --retry-delay 3 -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.14/postgresql-42.7.14.jar; \
    printf '%s\n' \
      'fba9eb73e6f0f5458355627fe095f5124705d4048551f4d6aa4084777b824c13  hadoop-aws-3.3.6.jar' \
      '5e51f896c86c922c254c0184d91a1957929d12c3f1d6fe17aae65909230bc125  aws-java-sdk-bundle-1.12.367.jar' \
      '73914527305a40cce504b0d3d90b23caf565136912d607ae8ae7c5895512332c  postgresql-42.7.14.jar' > SHA256SUMS; \
    sha256sum -c SHA256SUMS

FROM apache/hive:4.0.1
COPY --from=jars /jars/*.jar /opt/hive/lib/
COPY hive-site.xml /opt/hive/conf/hive-site.xml
