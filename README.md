# Repositório de Ferramentas de Ciência de Dados

Bem-vindo ao repositório de ferramentas de ciência de dados! Este repositório é uma coleção de códigos e scripts desenvolvidos para resolver desafios comuns em projetos de ciência de dados e engenharia de dados. O objetivo é fornecer soluções modulares, reutilizáveis e fáceis de implementar.

## Estrutura do Repositório

Abaixo está uma lista dos códigos planejados para este repositório:

1. **Pipeline Automatizado de Limpeza de Dados**  
   Um pipeline para realizar a limpeza automatizada de conjuntos de dados, lidando com valores ausentes, formatação de dados e detecção de outliers.

2. **Pipeline ETL (Extract, Transform, Load) Simples**  
   Um pipeline simples para extração, transformação e carregamento de dados entre diferentes fontes e destinos.

3. **Pacote Python Para Criação de Perfil de Dados**  
   Ferramenta para análise exploratória de dados que gera relatórios detalhados do perfil dos dados.

4. **Ferramenta CLI Para Gerar Ambientes de Projeto de Ciência de Dados**  
   Um utilitário de linha de comando para configuração rápida de ambientes de projeto com a estrutura e dependências necessárias.

5. **Pipeline Para Validação Automatizada de Dados**  
   Pipeline para verificar consistência, validade e integridade dos dados.

6. **Performance Profiler Para Funções Python**  
   Ferramenta para medir e otimizar o desempenho de funções Python.

7. **Ferramenta de Controle de Versão de Dados Para Modelos de Machine Learning**  
   Solução para rastrear alterações nos dados usados em experiências de aprendizado de máquina.

---

## Como Usar

### 1. Pipeline Automatizado de Limpeza de Dados
O arquivo `limpeza.py` contém o primeiro código deste repositório:

- **Funcionalidades:**  
  - Lida com valores ausentes usando estratégias como média, mediana, moda ou exclusão de linhas.
  - Formata dados em tipos específicos como `datetime` ou `float`.
  - Detecta e remove outliers com métodos como Z-score e IQR.
  - Integração com bancos de dados MySQL para leitura e gravação de dados processados.

- **Como Executar:**
  1. Configure as credenciais de banco de dados no dicionário `db_config`.
  2. Execute o arquivo `limpeza.py` com Python.
  3. O script busca os dados da tabela `historico`, processa-os e salva os resultados na tabela `historico_limpo`.

---

## Contribuições
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests para melhorar ou adicionar novas funcionalidades.

## Licença
Este repositório está licenciado sob a Licença MIT. Consulte o arquivo `LICENSE` para mais detalhes.

