# Projeto Final da Disciplina INF1802 - Big Data & Streaming

## Integração entre Cassandra e Kafka

Tópico dos tweets: tweets_topic
Para criá-lo:
```
kafka-topics --zookeeper $KAFKA_ZOOKEEPER_CONNECT --create --topic tweets_topic --partitions 3 --replication-factor 1
```

Antes de rodar o produtor, colocar as chaves do Twitter como variáveis de ambiente do projeto!
```
TWITTER_CONSUMER_KEY
TWITTER_CONSUMER_SECRET
TWITTER_ACCESS_TOKEN
TWITTER_ACCESS_TOKEN_SECRET
```

Caso haja erro com as trustAnchors, lembrar de adicionar o seguinte parâmetro nas configurações do programa (era necessário nas máquinas de treinamento):
```
-Djavax.net.ssl.trustStore=/etc/ssl/certs/java/cacerts
```

Após rodar o produtor e o consumidor, iniciar através de:
```
curl http://localhost:8080/tweets/collector
curl http://localhost:9080/tweets/consumer
```

To be continued...

[Imagem com select dos Tweets no Cassandra](tweets.png)
