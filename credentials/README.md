# Credentials

Эта папка содержит файлы с учетными данными для доступа к внешним сервисам.

## Файлы

- `credentials.json` - Google Cloud Service Account Credentials для доступа к Google Sheets API

## Важно!

⚠️ **Эта папка исключена из Git репозитория** для безопасности.

## Настройка

1. Создайте Service Account в Google Cloud Console
2. Скачайте JSON файл с учетными данными
3. Поместите его в эту папку как `credentials.json`
4. Убедитесь, что файл имеет правильные права доступа

## Структура credentials.json

```json
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "...",
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
  "client_email": "your-service-account@your-project.iam.gserviceaccount.com",
  "client_id": "...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "..."
}
```

## Безопасность

- Никогда не коммитьте файлы с учетными данными в Git
- Используйте переменные окружения в продакшене
- Регулярно ротируйте ключи доступа
