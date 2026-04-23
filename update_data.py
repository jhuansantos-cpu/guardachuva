"""
update_data.py
Consulta o BigQuery e gera um data.js com os valores atualizados.
Roda via GitHub Actions todo dia às 8h (horário de Brasília).
"""

import json
import os
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

# ── Configuração ──────────────────────────────────────────────
PROJECT   = "dm-mottu-aluguel"
DATASET   = "spc_estoque"
BRT       = timezone(timedelta(hours=-3))

client = bigquery.Client(project=PROJECT)

def hoje_brt():
    return datetime.now(BRT).strftime("%d/%m/%Y às %H:%M")

# ─────────────────────────────────────────────────────────────
# L3 — Inventário rotativo nos CDs
# Busca o snapshot mais recente disponível na tabela histórico
# ─────────────────────────────────────────────────────────────
def query_l3():
    sql = """
    WITH ultima_data AS (
        -- Pega a partição mais recente disponível
        SELECT MAX(DATE(atualizacao_dt, '-03:00')) AS dt
        FROM `dm-mottu-aluguel.spc_estoque.posicao_endereco_historico`
        WHERE UPPER(Local) LIKE '%CD-%'
          AND UPPER(Local) LIKE '%MOTTU%'
    ),
    snapshot AS (
        SELECT
            Local,
            MottuDescription,
            OriginalCode,
            SUM(Units)  AS total_units,
            MAX(price)  AS price_unit
        FROM `dm-mottu-aluguel.spc_estoque.posicao_endereco_historico`
        WHERE DATE(atualizacao_dt, '-03:00') = (SELECT dt FROM ultima_data)
          AND UPPER(Local) LIKE '%CD-%'
          AND UPPER(Local) LIKE '%MOTTU%'
          AND Units > 0
          -- Exclui México
          AND UPPER(Local) NOT LIKE '%MEXICO%'
          AND UPPER(Local) NOT LIKE '%CDMX%'
        GROUP BY Local, MottuDescription, OriginalCode
    )
    SELECT
        Local,
        MottuDescription,
        OriginalCode,
        ROUND(total_units, 0)                  AS qty,
        ROUND(price_unit, 2)                   AS preco,
        ROUND(total_units * price_unit, 2)     AS valor_total,
        (SELECT dt FROM ultima_data)           AS data_ref
    FROM snapshot
    WHERE total_units > 0
    ORDER BY valor_total DESC
    """
    rows = list(client.query(sql).result())
    if not rows:
        return None

    data_ref = str(rows[0]["data_ref"])
    itens = []
    for r in rows:
        itens.append({
            "cd":          r["Local"],
            "peca":        r["MottuDescription"],
            "codigo":      r["OriginalCode"],
            "qty":         int(r["qty"] or 0),
            "preco":       float(r["preco"] or 0),
            "valor_total": float(r["valor_total"] or 0),
        })

    total_valor = sum(i["valor_total"] for i in itens)
    total_qty   = sum(i["qty"] for i in itens)

    return {
        "data_ref":    data_ref,
        "total_valor": round(total_valor, 2),
        "total_qty":   total_qty,
        "itens":       itens,
    }


# ─────────────────────────────────────────────────────────────
# L4 — Descarte nas filiais (AI Agent)
# Fonte: z_ren_homologacao.descarte_intercom
# Preço cruzado com posicao_endereco_historico (últimos 30 dias)
# ─────────────────────────────────────────────────────────────
def query_l4():
    sql = """
    WITH descartes AS (
        SELECT
            PN_Limpo,
            original_code,
            motivo_descarte,
            numero_pecas,
            -- Trata dois formatos de data convivendo na mesma coluna:
            -- "DD/MM/YYYY HH:MM:SS" (len 19) e "YYYY-MM-DD ..." (len 24)
            CASE
                WHEN LENGTH(data_criacao) = 19
                    THEN PARSE_DATE('%d/%m/%Y', SUBSTR(data_criacao, 1, 10))
                ELSE SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(data_criacao, 1, 10))
            END AS data_dt
        FROM `dm-mottu-aluguel.z_ren_homologacao.descarte_intercom`
        WHERE numero_pecas IS NOT NULL
          AND data_criacao IS NOT NULL
    ),
    precos AS (
        -- Preço médio dos últimos 30 dias por SKU nos CDs Brasil
        SELECT
            OriginalCode,
            AVG(price) AS preco_medio
        FROM `dm-mottu-aluguel.spc_estoque.posicao_endereco_historico`
        WHERE DATE(atualizacao_dt, '-03:00') >= DATE_SUB(CURRENT_DATE('-03:00'), INTERVAL 30 DAY)
          AND price > 0
          AND UPPER(Local) LIKE '%CD-%'
          AND UPPER(Local) LIKE '%MOTTU%'
          AND UPPER(Local) NOT LIKE '%MEXICO%'
        GROUP BY OriginalCode
    ),
    agrupado AS (
        SELECT
            d.PN_Limpo,
            d.original_code,
            SUM(d.numero_pecas)                                 AS qty_total,
            ROUND(MAX(p.preco_medio), 2)                        AS preco_medio,
            ROUND(SUM(d.numero_pecas) * MAX(p.preco_medio), 2)  AS valor_estimado,
            MAX(d.data_dt)                                      AS ultima_data
        FROM descartes d
        LEFT JOIN precos p ON p.OriginalCode = d.PN_Limpo
        GROUP BY d.PN_Limpo, d.original_code
    )
    SELECT
        PN_Limpo,
        original_code,
        qty_total,
        preco_medio,
        valor_estimado,
        ultima_data
    FROM agrupado
    ORDER BY valor_estimado DESC NULLS LAST
    """
    rows = list(client.query(sql).result())
    if not rows:
        return None

    itens = []
    total_valor  = 0.0
    total_refugo = 0

    for r in rows:
        val  = float(r["valor_estimado"] or 0)
        qty  = int(r["qty_total"] or 0)
        total_valor  += val
        total_refugo += qty
        itens.append({
            "pn":           r["PN_Limpo"],
            "original_code": r["original_code"],
            "qty":          qty,
            "preco_medio":  float(r["preco_medio"] or 0),
            "valor_estimado": val,
            "ultima_data":  str(r["ultima_data"]) if r["ultima_data"] else None,
        })

    ticket_medio = round(total_valor / total_refugo, 2) if total_refugo else 0

    return {
        "total_valor":   round(total_valor, 2),
        "total_refugo":  total_refugo,
        "ticket_medio":  ticket_medio,
        "itens":         itens,
        "fonte":         "bigquery",
    }


# ─────────────────────────────────────────────────────────────
# Monta o payload e salva como data.js
# ─────────────────────────────────────────────────────────────
def main():
    print("🔍 Consultando BigQuery...")

    l3 = query_l3()
    l4 = query_l4()

    if not l3:
        print("⚠️  L3 sem dados — abortando")
        return

    payload = {
        "atualizado_em": hoje_brt(),
        "l3": l3,
        "l4": l4,
    }

    # Formata como arquivo JS que o index.html vai importar
    js_content = (
        "// Gerado automaticamente por update_data.py\n"
        "// Não editar manualmente.\n"
        f"const DADOS = {json.dumps(payload, ensure_ascii=False, indent=2)};\n"
    )

    output_path = os.path.join(os.path.dirname(__file__), "data.js")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(js_content)

    l3v = l3["total_valor"]
    l4v = l4["total_valor"]
    print(f"✅ data.js gerado com sucesso")
    print(f"   L3 → R$ {l3v:,.2f}  ({l3['total_qty']} itens)  ref: {l3['data_ref']}")
    print(f"   L4 → R$ {l4v:,.2f}  ({l4['total_refugo']} refugos)  fonte: {l4['fonte']}")
    print(f"   Total → R$ {l3v + l4v:,.2f}")
    print(f"   Atualizado em: {payload['atualizado_em']}")


if __name__ == "__main__":
    main()
