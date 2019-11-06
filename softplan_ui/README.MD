## Upload do arquivo

### Configuração

- Criar um arquvio credentials.env contendo as credenciais da aws
            
            aws_access_key_id = AWS KEY

            aws_secret_access_key = AWs SECRET KEY


- A aplicação nao possui login;
- O Arquvio CSV é enviado para um bucket S3 AWS;
- Assim que o Bucket recebe um arquivo um trigger é aciona uma funçao lambda para validar o arquivo


## Run app

            docker-compose up
ou
            
            flask run 

---

A apliçao possui as configurações necessárias deploy em uma maquina EBS

https://docs.aws.amazon.com/it_it/elasticbeanstalk/latest/dg/create-deploy-python-flask.html
