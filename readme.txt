﻿Задание:
    Написать сервис из 2х приложений: HTTP-acceptor`a и worker`а.
    HTTP-acceptor принимает запросы в формате json по роуту "/PUT" и кладёт их очередь rabbitMQ.
    Структура и содержание запросов произвольны. Важна корректная обработка ошибок при недоступности сервисов.
    Worker забирает данные из очереди и записывает их базу данных postgreSQL.
    Каждое приложение может быть запущено в нескольких экземплярах.

Самостоятельные уточнения:
    - приём запросов осуществляется методом "POST"
    - для удобства запуска нескольких экземпляров настройки передаются, как параметры запуска, 
        а не читаются из файла конфигурации
    - устойчивость очереди обеспечивается параметрами очереди durable и сообщения Persistent, транзакии
        и подтверждения не используются
    - тестируется только http-handler, так как у остального кода сильные внешние зависимости