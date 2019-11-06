## softplan_ui

Aplicação Flask com:<br>
*   um tela para upload de arquivo (upload)
*   um tela para consulta de arquivos (logs)

os arquivo sao carregados em um bucket s3.
O log dos arquivos carregados è salvado no DynamoDB

criar um bucket s3 ("softplan-s3")
e no arquivo [softplan_ui/config.env](softplan_ui/config.env)
configura o nome do bucket variavel 
BUCKET
<br>

---

O bucket s3 deve ter um trigger configurada para a funçao lambda

https://docs.aws.amazon.com/it_it/AmazonS3/latest/user-guide/enable-event-notifications.html

evento de acionamento da trigger.<br>
"All object create events"

para start da aplicação
[README.MD](softplan_ui/README.MD)

## validador_csv

Funçao lambda (AWS) para analise de arquivos

para start da aplicação
[README.MD](validador_csv/README.MD)