# AGENTS.md — pipeline

Пользовательский Go SDK Graphene и общая vocabulary сервера и агента. Модель
продукта находится в `../GRAPHENE.MD`.

## Перед изменением

1. Прочитайте `../GRAPHENE.MD`. Противоречащее ему изменение сначала правит
   продуктовый документ.
2. Перед push обязательны `make lint` и `make test`.

## Правила кода

- Go; код, имена и комментарии — на английском. Коммиты — Conventional Commits
  без `Co-Authored-By`.
- Идентификаторы — типы из `pkg/id` с суффиксом `Id`; исключение для имён
  переменных записано в `.golangci.yaml`.
- Секреты и большие данные не входят в specs, логи или Temporal history:
  используются ссылки из `pkg/ref`.
- Machine activities объявляются через `pkg/activity`; аргументы остаются
  сериализуемыми, а для небезопасного повтора выбирается явная гарантия.

## Границы пакетов

- `pkg/id`, `pkg/ref` — базовая vocabulary без Temporal и без импортов других
  пакетов этого репозитория.
- `pkg/wire` — межкомпонентные соглашения: queues, имена server activities и
  search attributes.
- `pkg/pipeline` — `Main`, handles ресурсов/агентов, run context, flows и
  встроенная CLI `plan`/`push`/`run`.
- `pkg/activity`, `pkg/artifact`, `pkg/file`, `pkg/trigger`, `pkg/obs` —
  пользовательские поверхности действий, данных, триггеров и телеметрии.
- `pkg/flow/*` — определения системных ресурсов и их `Ops` contracts;
  реализации `Ops` принадлежат серверу `graphene`.
- Серверный и агентский код в этот репозиторий не добавляются.
