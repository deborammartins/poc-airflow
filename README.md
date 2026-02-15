# poc-airflow

Repositório com objetivo de testar diferentes funcionalidades do Apache Airflow em conjunto com outras ferramentas e frameworks, como o Gusty, utilizando o ambiente local do Astronomer (Astro CLI).

🎯 Objetivos desta POC

> Testar DAGs dinâmicas com Gusty

> Explorar boas práticas de estruturação

> Validar integrações com GCP

> Experimentar organização modular de pipelines
---

## 🚀 Começando

Essas instruções permitirão que você obtenha uma cópia do projeto em operação na sua máquina local para fins de desenvolvimento e teste.

Consulte **Implantação** para saber como publicar o projeto em um ambiente remoto.

---

## 📋 Pré-requisitos

Antes de começar, você precisa ter instalado na sua máquina:

- Docker (Engine ou Desktop)
- Git
- Astronomer CLI (Astro)

### 🔹 Instalar Astro CLI (Linux)

```bash
curl -sSL https://install.astronomer.io | sudo bash
```

Verifique a instalação:

```python
astro version
```

### 📥 Clonar o repositório

```python
git clone https://github.com/deborammartins/poc-airflow.git
cd poc-airflow
```

### 🔧 Instalação e Execução Local

Este projeto utiliza o Astronomer Runtime para subir o Airflow via Docker.

▶️ 1. Subir o ambiente

Dentro da pasta do projeto:
```python
astro dev start
```
Na primeira execução o build pode demorar alguns minutos.

🌐 2. Acessar o Airflow

Após subir o ambiente, acesse:
http://localhost:8080


### 🔁 Parar o ambiente
```python
astro dev stop
```

## 🌪️ DAGs Dinâmicas com Gusty

Este projeto utiliza o Gusty para criação de DAGs a partir de arquivos YAML.

📦 Gerenciamento de Dependências

As dependências Python devem ser adicionadas em: requirements.txt

Após qualquer alteração:
```python
astro dev restart
```

🛠 Comandos úteis do Astro

| Comando             | Descrição                   |
| ------------------- | --------------------------- |
| `astro dev start`   | Inicia o ambiente local     |
| `astro dev stop`    | Para o ambiente             |
| `astro dev restart` | Reinicia o ambiente         |
| `astro dev logs`    | Exibe logs                  |
| `astro dev bash`    | Acessa o container          |
| `astro deploy`      | Deploy para ambiente remoto |


## 🚀 Implantação

Para realizar deploy em um ambiente remoto via Astronomer:
astro deploy

É necessário:

- Workspace configurado

- Deployment criado

- Login realizado via astro login


## 📚 Referências

- Documentação oficial do Apache Airflow:  
  https://airflow.apache.org/docs/

- Documentação do Astronomer:  
  https://www.astronomer.io/docs/

- Documentação do Gusty:  
  https://gusty.readthedocs.io/
