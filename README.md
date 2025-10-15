# Data Latte — Java ETL Pipeline

### Overview
Data Latte is a **Spring Boot 3** ETL that **extracts** data from a REST API, **transforms** it, and **loads** it into a **Kafka topic** (`etl.events`).  
Target: **MVP v0.1 by Oct 29 2025.**
---
### Tech Stack
- Java 17
- Spring Boot 3 (Web, Kafka)
- Kafka (single-node via Docker Compose)
- Maven 3.9+
- JUnit 5 / Mockito 5
- IntelliJ IDEA

---

### Prerequisites
```bash
java -version
mvn -version
docker --version && docker compose version
```

---

### Run Locally
```bash
# start Kafka (KRaft)
docker compose up -d

# run app
mvn spring-boot:run
```

To stop:
```bash
docker compose down
```
