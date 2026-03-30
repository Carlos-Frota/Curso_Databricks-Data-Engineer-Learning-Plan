# 🚀 Databricks Data Engineer Learning Plan - Laboratórios Práticos

Bem-vindo ao repositório de laboratórios práticos focados na preparação para a certificação **Databricks Certified Data Engineer Associate**. 

Este projeto nasceu da necessidade de ir além da teoria. Aqui, você encontrará a documentação e os códigos que mostram a evolução de um pipeline de dados no Databricks: saindo de um script inicial e "monolítico", passando pela refatoração modular com PySpark, até chegar à orquestração corporativa utilizando Databricks Workflows.

## 🎯 Objetivo do Projeto

O foco principal é compartilhar conhecimento e demonstrar as melhores práticas exigidas em ambientes corporativos de alto nível, ajudando outros profissionais a estruturarem seus estudos para a certificação. 

O projeto foi dividido em três fases:

### 🥉 Parte 1: Fundamentos e Arquitetura Medallion (Non Modularized Code)
O primeiro passo foi focar na lógica. Construímos um pipeline completo ingerindo dados brutos (CSV) do Unity Catalog Volumes e processando-os através das camadas **Bronze**, **Silver** e **Gold** em um único notebook.
* **Foco:** Validação de regras de negócio, ingestão e captura de metadados (`_metadata.file_name`).
* **Artigo no Medium:** [Lab 1: Non Modularized Code](https://medium.com/@carlosfrota_33970/lab-pyspark-modularized-code-22347713e8da)

### 🥈 Parte 2: Modularização e Escalabilidade (Design Modular)
Um código funcional não é necessariamente um código produtivo. Nesta fase, quebramos o "monolito" aplicando o princípio DRY (*Don't Repeat Yourself*).
* **Foco:** Criação de funções globais, passagem de parâmetros dinâmicos via `dbutils.widgets`, uso de UDFs e controle de evolução de schema (`mergeSchema` vs `overwriteSchema`) no Delta Lake.
* **Artigo no Medium:** [Lab 2: PySpark Modularized Code](https://medium.com/@carlosfrota_33970/lab-pyspark-modularized-code-22347713e8da)

### 🥇 Parte 3: Orquestração e Infraestrutura como Código (Workflows)
A etapa final para transformar scripts em um produto de dados autônomo.
* **Foco:** Construção de um DAG (Directed Acyclic Graph) para execução paralela utilizando **Databricks Workflows (Jobs)**. Configuração de dependências (`depends_on`), agendamento via *cron expressions*, otimização de performance e passagem de parâmetros via arquivo YAML.
* **Artigo no Medium:** [Lab 3: Orquestrando Pipelines de Dados com Databricks Workflows](https://medium.com/@carlosfrota_33970/parte-3-orquestrando-pipelines-de-dados-com-databricks-workflows-jobs)

---

## 🛠️ Tecnologias Utilizadas

* **Databricks Workspace:** Ambiente de desenvolvimento unificado.
* **Apache Spark (PySpark):** Motor de processamento de dados distribuído.
* **Delta Lake:** Camada de armazenamento que traz confiabilidade e ACID para data lakes.
* **Unity Catalog:** Governança e linhagem de dados.
* **Databricks Workflows (Jobs):** Orquestração e agendamento de tarefas via YAML.

---

## 📂 Estrutura do Repositório

```text
📦 Curso_Databricks-Data-Engineer-Learning-Plan
 ┣ 📂 Parte_1_Non_Modularized
 ┃ ┗ 📜 PySpark_Non_Modular_Code.py
 ┣ 📂 Parte_2_Modularized
 ┃ ┣ 📜 PySpark_Modularized_Code_Functions.py
 ┃ ┣ 📜 PySpark_Modularized_Code_Bronze.py
 ┃ ┣ 📜 PySpark_Modularized_Code_Silver.py
 ┃ ┗ 📜 PySpark_Modularized_Code_Gold.py
 ┣ 📂 Parte_3_Orchestration
 ┃ ┗ 📜 databricks_workflow_job.yml
 ┗ 📜 README.md