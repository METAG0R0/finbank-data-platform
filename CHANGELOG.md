# CHANGELOG

## [1.0.0] - 2026-09-14

### Added

- Implementación de plataforma de datos FinBank sobre Databricks.
- Generación de datos sintéticos para las entidades del escenario bancario.
- Fuente relacional mediante Lakebase PostgreSQL.
- Capa Bronze para ingesta y preservación del origen.
- Capa Silver para limpieza, deduplicación, integridad referencial y protección de PII.
- Detección de transacciones sospechosas mediante comportamiento histórico de 30 días.
- Capa Gold con dimensiones y tablas de hechos.
- KPIs de cartera y agregaciones analíticas.
- Cálculo de `bucket_mora`.
- Cálculo de `cltv_12m_comisiones` a partir de comisiones cobradas.
- Validaciones de calidad sobre las estructuras procesadas.
- Metadata técnica para trazabilidad de las ejecuciones.
- Documentación técnica de los notebooks mediante exportaciones HTML.
- Evidencia visual del catálogo de tablas generado en Databricks.

### Technical Decisions

- Arquitectura Medallion: Bronze → Silver → Gold.
- Delta Lake como formato de persistencia analítica.
- PySpark como motor principal de transformación.
- Lakebase PostgreSQL como fuente SQL.
- Hash SHA-256 para protección del número de documento.
- `ind_sospechoso` calculado en Silver para mantener la regla de detección próxima a la capa de calidad y conformación.
- No se incorporan fuentes ni reglas de negocio que no estén disponibles en los datos de entrada.

### Limitations

- La ejecución se realizó sobre Databricks Free Edition, con limitaciones de cómputo.
- El cálculo completo de rentabilidad utiliza únicamente las comisiones cobradas disponibles en la fuente.
- No se estiman intereses ni tipos de cambio cuando no existe información de origen suficiente.
- Las capacidades no ejecutadas o no evidenciadas en el entorno disponible no se presentan como implementadas.