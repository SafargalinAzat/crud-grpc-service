# Профильное задание: gRPC - Tarantool сервис

gRPC-сервис для работы с Key-Value хранилищем на базе Tarantool.

## Описание

Сервис реализует следующее API:
- **Put** - сохраняет значение по ключу или обновляет существующее значение
- **Get** - получает значение по ключу
- **Delete** - удаляет значение по ключу
- **Range** - получает диапазон значений между ключами (потоковая передача)
- **Count** - считает количество элементов в спейсе



## Запуск

Запустить можно как локально, так и в докер компоуз с двумя контейнерами - grpc сервером и контейнером с Tarantool и общей сетью.

### Docker Compose

```bash
docker compose up --build -d
```

Сервис по умолчанию доступен на `localhost:8080`, Tarantool на `localhost:3301`.
(Найстройки портов можно изменить в .env, пример .env - env.example)

### Локальный запусе

```bash
mvn clean package

java -jar target/crud-grpc-service-1.0.0.jar
```

## Тестирование

Для тестирования используются Python-скрипты в директории `python_tests/`:

```bash
pip install -r python_tests/requirements.txt

python python_tests/testcrud.py
```

## Структура проекта

```
src/main/java/com/vktest/grpc/   исходный код сервиса
src/main/proto/                  определения protobuf
python_tests/                    тесты на Python
```

## Конфигурация

Параметры сервиса задаются через переменные окружения в `.env`.
