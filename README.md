# mysql-migrate

CLI para migrar tabelas de um banco MySQL/MariaDB (origem) para outro (destino), com:
- Streaming e INSERT em batches (várias linhas por vez) para alta performance
- Execução paralela com limite de concorrência por tabela
- Barra/log de progresso por tabela
- Ordenação alfabética de tabelas
- Verificação opcional de contagem (origem vs destino) ao final de cada tabela

Como funciona (resumo)
- Para cada tabela (em paralelo até CONCURRENCY):
  1) Captura o DDL na origem e recria a tabela no destino (DROP + CREATE)
  2) Faz SELECT streaming na origem
  3) Acumula linhas até BATCH_SIZE e executa um único INSERT multi-values
  4) Repete até terminar; ao final, opcionalmente verifica a contagem src/dst

Observações de performance
- BATCH_SIZE controla quantas linhas por INSERT. Padrão: 1000.
  - Números maiores aumentam throughput, mas cuidado com max_allowed_packet e memória.
  - Ajuste gradualmente: 1000 → 5000 → 10000, conforme comportamento do servidor.
- CONCURRENCY controla quantas tabelas migram em paralelo (2–4 é um bom ponto de partida).
- A estimativa de progresso usa INFORMATION_SCHEMA.TABLES (TABLE_ROWS) e pode ser aproximada em InnoDB.

## Instalação

```bash
npm install
```

Opcionalmente, torne o binário executável globalmente com `npm link`:

```bash
npm link
# Agora: mysql-migrate --help
```

## Configuração

Crie um arquivo `.env` baseado em `.env.example`:

```
SRC_HOST=localhost
SRC_PORT=3306
SRC_USER=root
SRC_PASSWORD=secret
SRC_DATABASE=origem_db

DST_HOST=localhost
DST_PORT=3306
DST_USER=root
DST_PASSWORD=secret
DST_DATABASE=destino_db

CONCURRENCY=4
BATCH_SIZE=1000
INCLUDE_TABLES=
EXCLUDE_TABLES=
VERIFY=1
DRY_RUN=0
SKIP_CONNECT=0
```

## Uso

- Canário (poucas tabelas) para validar conectividade e progresso:

```bash
INCLUDE_TABLES=users,roles CONCURRENCY=2 BATCH_SIZE=2000 npm start
```

- Migrar tudo:

```bash
npm start
```

## Opções úteis
- CONCURRENCY=N: número de tabelas processadas em paralelo
- BATCH_SIZE=N: linhas por INSERT (multi-values)
- INCLUDE_TABLES/EXCLUDE_TABLES: filtra tabelas
- VERIFY=1: verifica contagem origem/destino ao final de cada tabela
- DRY_RUN=1: simula sem conectar

## Limitações e dicas
- O progresso em % é aproximado com InnoDB; o valor final e a verificação por contagem são a fonte de verdade.
- Transação é por tabela; para tabelas enormes, isso pode gerar transações longas. Se necessário, podemos alternar para “commit por batch” (melhora latência/locks com custo de atomicidade por tabela).
- max_allowed_packet no destino deve suportar o tamanho dos batches escolhidos.

## Scripts
- `npm start` ou `npm run migrate`: roda a migração
- `npm run dry`: modo simulação (DRY_RUN=1)

## Docker

Para executar a migração usando Docker, primeiro construa a imagem:

```bash
docker build -t mysql-migrate .
```

Para executar a migração, mapeie o arquivo `.env` do host para o container. Se o arquivo `.env` não existir no diretório atual, o container criará um baseado no `.env.example`:

```bash
docker run --rm -v $(pwd)/.env:/app/.env mysql-migrate
```

Os logs serão exibidos no terminal, da mesma forma que ao executar `node src/migrate.js` localmente.

Certifique-se de que o arquivo `.env` contenha as configurações necessárias, como hosts, usuários e senhas dos bancos de dados de origem e destino.

## Licença

ISC
