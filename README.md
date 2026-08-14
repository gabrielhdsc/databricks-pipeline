# Ingestao Medallion no Databricks

Este projeto implementa uma carga de dados em arquitetura medallion, executada pelo job `Pipeline_bronze_to_gold` definido em `databricks.yml`.

O objetivo principal deste repositório é demonstrar o domínio sobre o uso de **Databricks Asset Bundles (DABs)** como Infraestrutura como Código (IaC) e do **Auto Loader** para ingestão eficiente e resiliente de dados.

1. **Bronze**: usa Auto Loader para ingerir arquivos JSON de `past_rates` e `listings` da landing zone, preservando metadados de ingestao e dados resgatados por evolucao de schema.
2. **Silver**: consome `past_rates` da Bronze como stream, tipa as colunas e remove metadados tecnicos.
3. **Gold**: gera metricas mensais por cidade na tabela `monthly_city_listings_metrics`.

## Estrutura

- `src/01_bronze.py`, `src/02_silver.py` e `src/03_gold.py`: notebooks Python chamados sequencialmente pelo job.
- `src/Batches_creation.py`: utilitario manual para criar lotes JSON de teste na landing zone.
- `databricks.yml`: Databricks Asset Bundle e definicao do job.
- `resources/`: arquivos remanescentes do template; nao sao incluidos pelo bundle atual.
- `tests/`: teste de exemplo do template para o modulo `databricks_ingestion`.

## Pre-requisitos

- Um workspace Azure Databricks com Unity Catalog habilitado.
- Catalogo `projeto_teste` (ou a configuracao equivalente ajustada no codigo e no bundle).
- Permissao para criar schemas, volumes e tabelas Delta nesse catalogo.
- Databricks CLI autenticada e `uv` instalados para uso local.

Instale as dependencias de desenvolvimento:

```bash
uv sync --dev
```

## Dados de entrada

Antes de executar o job, disponibilize arquivos JSON nos caminhos abaixo. Os volumes sao criados pela etapa Bronze, mas os arquivos precisam ser carregados posteriormente.

```text
/Volumes/projeto_teste/bronze_teste/landingzone/json_past_rates/
/Volumes/projeto_teste/bronze_teste/landingzone/json_listings/
```

Para dados CSV de exemplo, execute `src/Batches_creation.py` no Databricks. Ele foi desenhado para simular o comportamento de um sistema de origem real. Ele atua de duas formas vitais para homologar o pipeline:
    1 - Simulação de Fluxo Contínuo: Ele lê arquivos CSV estáticos (listings.csv e past_rates.csv) e os particiona em arquivos JSON menores (lotes/batches). Isso simula o comportamento de arquivos caindo de forma contínua em um storage.

    2 - Teste de Evolução de Schema (Drift): Ao gerar o segundo lote de dados, o script adiciona propositalmente uma coluna não esperada        (extra_tax). O objetivo é provar na prática a eficácia do Rescue Mode do Auto Loader: o pipeline absorve o novo schema sem falhar e isola os dados novos de forma segura.

## Deploy e execução

Valide o bundle antes do deploy:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run pipeline_medallion_architecture --target dev
```

Para producao, revise primeiro o catalogo, o host, o usuario configurado e o tamanho do cluster em `databricks.yml`; em seguida execute `deploy` com `--target prod`.

## Desenvolvimento e testes

```bash
uv run pytest
```

O teste atual usa Databricks Connect e requer credenciais e compute acessivel. Ele e um teste do codigo de exemplo, nao uma validacao ponta a ponta da ingestao.

## Destaques Técnicos e Decisões de Arquitetura

- Adoção de ingestão incremental (streaming) nas camadas Bronze e Silver para eficiência, combinada com recálculo total (`overwrite`) na camada Gold. Garante idempotência, simplifica agregações complexas e corrige automaticamente dados com chegada tardia (*late-arriving data*) sem a alta complexidade de gerenciar um `MERGE INTO`.
- As etapas Bronze e Silver usam `availableNow` e aguardam o termino do stream antes de finalizar a tarefa; isso preserva a ordem Bronze -> Silver -> Gold no job.
- Configuração do Auto Loader com `schemaEvolutionMode="rescue"`. Isso garante que novas colunas adicionadas na origem não quebrem o pipeline (dados não mapeados vão para a coluna `_rescued_data`).
- Arquitetura construída 100% sobre o **Unity Catalog**, utilizando *Volumes* para a aterrissagem de arquivos brutos.
