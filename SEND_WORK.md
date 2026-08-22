# Entrega do Trabalho

---

## Checklist antes de entregar

Execute esta verificação antes de qualquer push final:

- [ ] A pipeline processa todas as coleções com o mesmo código
- [ ] Pelo menos uma coleção usa carga incremental com watermark persistida
- [ ] A tabela `control_ingestion_log` existe e tem registros de pelo menos **3 execuções**
- [ ] As colunas de rastreabilidade (R4) estão presentes em todas as tabelas Bronze
- [ ] Evidências de execução estão salvas (screenshots ou saída de query)
- [ ] O README da solução tem o diagrama de arquitetura
- [ ] Nenhuma credencial está no repositório — execute: `git log -p | grep -i "password\|uri\|secret\|token"` e confirme vazio
- [ ] Registro de contribuição individual preenchido (ver seção abaixo)

---

## Estrutura esperada do repositório entregue

O avaliador vai procurar exatamente nesses locais:

```
.
├── README.md              ← arquitetura, decisões técnicas, como executar
│
├── config/
│   ├── pipeline_config.yaml      ← configuração da pipeline (sem credenciais)
│   └── collections.json          ← parâmetros por coleção
│
├── jobs/
│   ├── ingestion_job.py
│   └── bronze_job.py
│
├── notebooks/
│   └── (seus notebooks de desenvolvimento e evidências)
│
├── docs/
│   ├── ARQUITETURA.md     ← diagrama Mermaid ou imagem + descrição
│   └── evidencias/        ← screenshots das execuções obrigatórias
│       ├── execucao_01_full_load.png
│       ├── execucao_02_incremental_sem_novidades.png
│       └── execucao_03_incremental_com_dados.png
│
└── CONTRIBUICOES.md       ← registro individual de quem fez o quê
```

---

## Como fazer a entrega

### Fork + Pull Request (preferencial)

1. Faça o **fork** do repositório original no GitHub
2. Trabalhe no fork do seu grupo (todos os membros com acesso de escrita)
3. Quando finalizado, abra um **Pull Request** do seu fork para o repositório original
4. No título do PR: `[Trabalho Final] Nome do Grupo — Turma XX`
5. No corpo do PR, cole a saída do `control_ingestion_log` das 3 execuções obrigatórias


---

## Registro de contribuição individual

Crie o arquivo `CONTRIBUICOES.md` na raiz do repositório com o seguinte formato:

```markdown
# Registro de Contribuições

## Grupo: <nome do grupo>

| Membro | Matrícula | Contribuições principais |
|--------|-----------|--------------------------|
| Nome 1 | XXXXXX | extractor.py, configuração do Atlas, notebooks de exploração |
| Nome 2 | XXXXXX | loader.py, estratégia de idempotência, docs/ARQUITETURA.md |
| Nome 3 | XXXXXX | control.py, watermark, evidências de execução |

## Detalhamento por commit

> Cole aqui a saída de: git log --oneline --author="Nome" para cada membro
```

Membros sem contribuição identificável no histórico de commits e sem descrição no `CONTRIBUICOES.md` perdem pontos conforme rubrica.

---

## Evidências de execução obrigatórias

Você precisa demonstrar **três execuções distintas**:

### Execução 1 — Carga full inicial
- Todas as 6 coleções processadas
- `control_ingestion_log` mostra `load_type = full` para as coleções configuradas como full
- Contagens: `qtd_lida_origem` deve bater com os volumes documentados

### Execução 2 — Carga incremental sem novidades
- Execute novamente a pipeline para a coleção incremental
- `qtd_lida_origem` deve ser 0 (sem registros novos após a watermark)
- `status = SUCCESS`

### Execução 3 — Carga incremental com dados novos
- Insira manualmente 1–3 documentos na coleção de comentários com `date` após a watermark atual
- Execute a pipeline
- `qtd_lida_origem` deve refletir apenas os novos registros
- Bronze deve ter os novos registros; registros anteriores não duplicados

**Salve as evidências em `docs/evidencias/`** — prints do terminal ou saída de query sobre o `control_ingestion_log`.