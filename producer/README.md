# TweetCollectorApplication

Nesse exercício, usaremos a API de stream do Twitter para coletar Tweets que possuam determinadas palavras.

Para deixar o exercício mais interessante, a solução vai usar um serviço REST que inicia e finaliza a coleta de Tweets.
O serviço é iniciado com:
> curl http://localhost:8080/tweets/collector

O serviço é finalizado com:
> curl --request DELETE http://localhost:8090/tweets/collector

A implementação do serviço REST já está implementada e você precisará apenas desenvolver as classes que gerenciam o
ciclo de vida do serviço e o listener de coleta dos tweets.

## Passo 1:

Implemente uma classe que representa um Tweet e possua os seguintes campos:

* O nome do usuário que enviou o tweet
* O texto do tweet
* A data em que o tweet foi enviado

## Passo 2:

Implemente o listener que recebe os tweets.
A cada tweet recebido, o listener deve criar um objeto Tweet e escrever na console.

## Passo 3:

Implemente a classe que gerencia o ciclo de vida do serviço.

A interface *LifecycleManager* define os métodos *start* e *stop* que são chamados
pelos *endpoints* que iniciam e finalizam o serviço REST.

A sua classe deve implementar a interface *LifecycleManager* e a interface de java *Serializable*.

Atualize a classe fornecida *InjectionBinder* para fazer a injeção no serviço usando a sua classe.

## Passo 4:

Sua classe, que implementa o *LifecycleManager*, deve ser encarregar de configurar a autenticação para uso da API do Twitter, 
conforme os dados de "Keys and tokens" no registro de sua aplicação no Twitter. Você pode usar variáveis de ambiente, conforme
o exemplo abaixo:

~~~~
        String _consumerKey = System.getenv().get("TWITTER_CONSUMER_KEY");
        String _consumerSecret = System.getenv().get("TWITTER_CONSUMER_SECRET");
        String _accessToken = System.getenv().get("TWITTER_ACCESS_TOKEN");
        String _accessTokenSecret = System.getenv().get("TWITTER_ACCESS_TOKEN_SECRET");

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.setOAuthConsumerKey(_consumerKey)
                .setOAuthConsumerSecret(_consumerSecret)
                .setOAuthAccessToken(_accessToken)
                .setOAuthAccessTokenSecret(_accessTokenSecret);
~~~~ 

## Passo 5: 

A mesma classe acima, deve criar um *TwitterStream* que será usado para iniciar e suspender
a recepção de tweets nos métodos de, respectivamente, *start* e *stop*.

## Passo 6: 

Crie um filtro para receber apenas tweets que possuam palavras que você escolha.
Essas palavras podem também estar configuradas em uma variável de ambiente.

## Passo 7:

Caso você queira gerar o jar de sua aplicação, com todas as bibliotecas embutidas:

> mvn install assembly:assembly

## Passo 8:

Para executar o seu programa Java pela linha de comando:

> java -cp target/twitter-exercise-1.0-SNAPSHOT-jar-with-dependencies.jar TweetCollectorApplication

Não esquece de configurar as variáveis de ambiente antes de executar seu programa.
