# -*- coding: utf-8 -*-
"""
cda_online_dynamox — coleta da plataforma Dynamox (UBU e GERMANO) e carga no
Google Sheets.

Etapas (isoladas; a falha de uma não derruba as independentes):
  hierarquia (workspaces + assets + pontos), alertas, laudos (D-1, upsert),
  telemetria (D-1, incremental).
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dtime, timedelta, timezone
import ast
import threading

import jwt
import pandas as pd

from cda_utils import (
    Pipeline, abrir_aba, anexar_novos, dia_referencia, env_json,
    env_obrigatoria, gravar_aba, http, log, sheets_retry,
)

# ---------------------------------------------------------------------------
# 1. CONFIGURAÇÃO
# ---------------------------------------------------------------------------

API = "https://api.dynamox.solutions"
APPLICATION_KEY = env_json("DYNAMOX_SERVICE_ACCOUNT")
TOKEN_URL = env_obrigatoria("DYNAMOX_TOKEN")
WORKSPACES = {
    "ubu": env_obrigatoria("DYNAMOX_UBU"),
    "germano": env_obrigatoria("DYNAMOX_GERMANO"),
}
WORKERS = 3          # a Dynamox devolve 429 com facilidade: paralelismo baixo

# Máquinas cuja telemetria (temperatura) é coletada
MACHINELIST = ["62b20314457a4c772a1ba4a8"]

PLANILHAS = {
    "hierarquia": "1yh0_mkYbB-KbwUNwz9txY99bQHktD9mUFtDVscbOLag",
    "alertas": "1DUmtzyd6WpsSYm9W5CWuyiQ_9-FkRbZz_luHhkH-CeI",
    "laudos": "1x6AJxILgPxb13LCaK1L8qizZaxGK3qOZtTiR_BwqyoA",
    "telemetria": "11rzDwr5U6HAfIUXHLNOCcoFkmPEcOUNDpTnZ4Nmg2uA",
}

# ---------------------------------------------------------------------------
# 2. AUTENTICAÇÃO (token renovado automaticamente em 401)
# ---------------------------------------------------------------------------

_headers = {}
_lock = threading.Lock()


def renovar_token():
    assinatura = jwt.encode(
        headers={"kid": APPLICATION_KEY["_id"], "alg": "RS256", "typ": "JWT"},
        payload={
            "iat": int(datetime.now(timezone.utc).timestamp()),
            "email": APPLICATION_KEY["email"],
        },
        key=APPLICATION_KEY["privateKey"].encode("utf-8"),
        algorithm="RS256",
    )
    resp = http("POST", TOKEN_URL, headers={"Authorization": f"Bearer {assinatura}"})
    dados = resp.json()
    if "access_token" not in dados:
        # não imprime o corpo inteiro (pode conter dados sensíveis)
        raise RuntimeError(f"Resposta de token sem access_token (chaves: {list(dados)})")
    with _lock:
        _headers.clear()
        _headers.update({
            "Authorization": f"Bearer {dados['access_token']}",
            "Content-Type": "application/json",
        })
    log.info("Token Dynamox obtido")   # o token NÃO é mais impresso no log
    return dict(_headers)


def api_get(caminho_ou_url, params=None, aceitar_404=False):
    url = caminho_ou_url if caminho_ou_url.startswith("http") else f"{API}{caminho_ou_url}"
    try:
        resp = http(
            "GET", url,
            headers=dict(_headers) or renovar_token(),
            params=params,
            ao_401=renovar_token,
            tentativas=8,
            espera_max=300,
        )
    except Exception as exc:  # noqa: BLE001
        r = getattr(exc, "response", None)
        if aceitar_404 and r is not None and r.status_code == 404:
            return None
        raise
    return resp.json()


def paginar_docs(caminho, params=None):
    """Paginação por page/hasNext usada em assets, pontos e laudos."""
    params = dict(params or {})
    pagina, docs = 1, []
    while True:
        params["page"] = pagina
        data = api_get(caminho, params) or {}
        docs.extend(data.get("docs") or [])
        if not (data.get("pages") or {}).get("hasNext"):
            return docs
        pagina += 1


def em_paralelo(func, itens, tolerancia=0.02):
    itens = list(itens)
    falhas = []

    def seguro(item):
        try:
            return func(item)
        except Exception as exc:  # noqa: BLE001
            falhas.append(item)
            log.error("Falha em %s: %s", item, exc)
            return None

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        resultados = list(pool.map(seguro, itens))
    if itens and len(falhas) / len(itens) > tolerancia:
        raise RuntimeError(f"{len(falhas)}/{len(itens)} requisições falharam — etapa abortada")
    if falhas:
        log.warning("%d/%d itens falharam (dentro da tolerância)", len(falhas), len(itens))
    return resultados


def parse_field(val):
    """Converte string de dict/list para objeto Python."""
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except (ValueError, SyntaxError):
            return val
    return val


# ---------------------------------------------------------------------------
# 3. HIERARQUIA
# ---------------------------------------------------------------------------

def list_children(parent_id):
    filhos, page_token = [], None
    while True:
        params = {"parentId": parent_id, "limit": 100}
        if page_token:
            params["pageToken"] = page_token
        data = api_get("/v1/workspaces", params)  # erro -> exceção (antes: árvore incompleta silenciosa)
        filhos.extend(data.get("docs", []))
        page_token = (data.get("items") or {}).get("nextPageToken")
        if not page_token:
            return filhos


def traverse(ws_id, ws_name, ancestors=None):
    """Retorna a lista de caminhos completos até cada folha."""
    caminho = (ancestors or []) + [{"id": ws_id, "name": ws_name}]
    filhos = list_children(ws_id)
    if not filhos:
        return [caminho]
    resultado = []
    for filho in filhos:
        resultado.extend(traverse(filho["_id"], filho["name"], caminho))
    return resultado


def montar_workspaces():
    rows = []
    for site, root_id in WORKSPACES.items():
        folhas = traverse(root_id, site)
        log.info("%s: %d folhas", site, len(folhas))
        for path in folhas:
            for i in range(len(path)):
                row = {"site": site}
                for nivel, ws in enumerate(path[: i + 1]):
                    row[f"workspace.{nivel}.id"] = ws["id"]
                    row[f"workspace.{nivel}.name"] = ws["name"]
                row["last_workspaceId"] = path[i]["id"]
                row["last_workspaceName"] = path[i]["name"]
                rows.append(row)
    return pd.DataFrame(rows).drop_duplicates(subset=["last_workspaceId"])


def coletar_assets():
    todos = []
    for nome, ws_id in WORKSPACES.items():
        docs = paginar_docs(f"/v1/workspaces/{ws_id}/assets")
        for d in docs:
            d["workspaceId"] = ws_id
            d["workspaceName"] = nome
        log.info("%s: %d assets", nome, len(docs))
        todos.extend(docs)
    if not todos:
        raise RuntimeError("Nenhum asset retornado")
    return pd.DataFrame(todos)


def coletar_pontos(df_assets):
    raiz = df_assets[pd.to_numeric(df_assets["depthAsset"], errors="coerce") == 0]
    linhas = list(raiz[["_id", "workspaceId", "workspaceName"]].itertuples(index=False))

    respostas = em_paralelo(
        lambda l: paginar_docs(f"/v1/assets/{l[0]}/monitoring-points", {"limit": 100}),
        linhas,
    )
    pontos = []
    for (asset_id, ws_id, ws_name), docs in zip(linhas, respostas):
        for d in docs or []:
            d["assetId"] = asset_id
            d["workspaceId"] = ws_id
            d["workspaceName"] = ws_name
            pontos.append(d)
    log.info("Total de pontos coletados: %d", len(pontos))
    return pd.DataFrame(pontos)


def parse_sensors(sensors_raw):
    """Extrai x, y, z do campo sensors (string ou lista)."""
    try:
        sensors_raw = parse_field(sensors_raw)
        if sensors_raw:
            axes = sensors_raw[0].get("axesOrientation", {})
            return axes.get("x"), axes.get("y"), axes.get("z")
    except (AttributeError, IndexError, KeyError, TypeError):
        pass
    return None, None, None


def montar_hierarquia(df_workspaces, df_assets, df_points):
    at_idx = df_assets.set_index("_id").to_dict("index")
    ws_idx = df_workspaces.set_index("last_workspaceId").to_dict("index")
    ws_cols = sorted(c for c in df_workspaces.columns if c.startswith("workspace."))

    def subir(point):
        row = {
            "spot.id": point.get("_id"),
            "spot.name": point.get("name"),
            "spot.createdAt": point.get("createdAt"),
            "spot.updatedAt": point.get("updatedAt"),
        }
        row["sensor_x"], row["sensor_y"], row["sensor_z"] = parse_sensors(point.get("sensors"))

        # Caso normal:  ponto.parentId -> depth1/depth2 -> depth0
        # Caso especial (bug da plataforma): parentId == _id -> usa assetId (depth0)
        parent_id = point.get("parentId")
        asset_id = point.get("assetId")
        auto_ref = parent_id == point.get("_id")
        depth0 = depth1 = depth2 = None
        depth1_id = None

        if auto_ref:
            depth0 = at_idx.get(asset_id)
        else:
            first = at_idx.get(parent_id)
            if first:
                d = str(first.get("depthAsset", ""))
                if d == "2":
                    depth2 = {**first, "_id": parent_id}
                    depth1_id = first.get("parentId")
                    depth1 = at_idx.get(depth1_id)
                    if depth1:
                        depth1 = {**depth1, "_id": depth1_id}
                        depth0 = at_idx.get(depth1.get("parentId"))
                elif d == "1":
                    depth1 = {**first, "_id": parent_id}
                    depth0 = at_idx.get(first.get("parentId"))
                elif d == "0":
                    depth0 = first

        subset = depth2 or depth1
        if subset:
            row["subset.id"] = subset.get("_id") or parent_id
            row["subset.name"] = subset.get("name")
        if depth1 and depth2:
            row["component.id"] = depth1.get("_id")
            row["component.name"] = depth1.get("name")

        if depth0:
            row["machine.id"] = asset_id if auto_ref else (depth1.get("parentId") if depth1 else parent_id)
            row["machine.name"] = depth0.get("name")
            ws_parent = depth0.get("parentId")
            ws = ws_idx.get(ws_parent)
            if ws:
                row["last_workspaceId"] = ws_parent
                row["last_workspaceName"] = ws.get("last_workspaceName")
                for col in ws_cols:
                    row[col] = ws.get(col)
        return row

    df = pd.DataFrame([subir(p) for p in df_points.to_dict("records")])
    ordem = (
        sorted(c for c in df.columns if c.startswith("workspace."))
        + ["last_workspaceId", "last_workspaceName",
           "machine.id", "machine.name", "machine.configurationType",
           "subset.id", "subset.name", "component.id", "component.name",
           "spot.id", "spot.name", "sensor_x", "sensor_y", "sensor_z",
           "spot.createdAt", "spot.updatedAt"]
    )
    df = df[[c for c in ordem if c in df.columns]]
    if "workspace.0.id" in df.columns:
        log.info("Linhas sem workspace: %d", df["workspace.0.id"].isna().sum())
    return df


def etapa_hierarquia():
    df_ws = montar_workspaces()
    df_assets = coletar_assets()
    df_points = coletar_pontos(df_assets)
    if df_points.empty:
        raise RuntimeError("Nenhum ponto de monitoramento retornado")
    df = montar_hierarquia(df_ws, df_assets, df_points)
    log.info("Hierarquia: %s", df.shape)
    gravar_aba(PLANILHAS["hierarquia"], df, "hierarquia")
    return df


# ---------------------------------------------------------------------------
# 4. ALERTAS (status das alert-policies por ponto)
# ---------------------------------------------------------------------------

def etapa_alertas(df_hier):
    spots = df_hier["spot.id"].dropna().unique().tolist()
    log.info("Total de spots: %d", len(spots))

    def buscar(spot):
        return api_get("/v1/alert-policies/status", {"resourceId": spot}, aceitar_404=True)

    respostas = em_paralelo(buscar, spots)
    nao_encontrados = sum(r is None for r in respostas)
    if spots and nao_encontrados == len(spots):
        # Acontecia todo dia: 404 em 100% dos pontos e a planilha era
        # apagada e regravada vazia. Agora a planilha é preservada.
        raise RuntimeError(
            "404 em TODOS os pontos no endpoint /v1/alert-policies/status — "
            "verifique com a Dynamox se o endpoint/parâmetro mudou. Planilha preservada."
        )

    politicas = []
    for spot, lista in zip(spots, respostas):
        for pol in lista or []:
            pol["spotId"] = spot
            politicas.append(pol)
    df = pd.DataFrame(politicas)
    log.info("Total de policies: %d (%d pontos sem policy/404)", len(df), nao_encontrados)
    gravar_aba(PLANILHAS["alertas"], df, "alertas")
    return df


# ---------------------------------------------------------------------------
# 5. LAUDOS (technical reports do dia de referência — upsert por reportId)
# ---------------------------------------------------------------------------

COLUNAS_LAUDOS = [
    "reportId", "reportedAt", "detected", "resolution", "status",
    "open", "inProgress", "closed",
    "machine", "machineId", "subset", "subsetId", "component", "componentId",
    "spot", "spotId", "diagnostic", "failures", "action",
    "noteNumber", "statusNote", "observation", "deadline", "criticality",
    "type", "userCreated", "createdAt", "lastUserUpdated", "lastUserUpdateAt",
    "deleted", "deletedAt", "updatedAt",
]


def processar_laudo(doc):
    row = {
        "reportId": doc.get("_id"),
        "status": doc.get("alertLevel"),
        "resolution": doc.get("status"),
        "type": doc.get("type"),
        "noteNumber": doc.get("noteNumber"),
        "statusNote": doc.get("statusNote"),
        "deadline": doc.get("deadline"),
        "criticality": doc.get("criticality"),
        "deleted": doc.get("deleted"),
        "createdAt": doc.get("createdAt"),
        "reportedAt": doc.get("reportedAt"),
        "detected": doc.get("detected"),
        "lastUserUpdateAt": doc.get("lastUserUpdateAt"),
        "updatedAt": doc.get("updatedAt"),
        "deletedAt": doc.get("deletedAt"),
        "diagnostic": doc.get("diagnostic"),
        "action": doc.get("action"),
        "observation": doc.get("note"),
    }

    historico = parse_field(doc.get("statusHistory", {}))
    if isinstance(historico, dict):
        row["open"] = historico.get("open")
        row["inProgress"] = historico.get("inProgress")
        row["closed"] = historico.get("closed")

    failures = parse_field(doc.get("failures", []))
    if isinstance(failures, list):
        row["failures"] = ", ".join(str(f) for f in failures if f != "none") or None
    else:
        row["failures"] = failures

    user = parse_field(doc.get("user", {}))
    row["userCreated"] = user.get("name") if isinstance(user, dict) else None
    user = parse_field(doc.get("lastUpdatedBy", {}))
    row["lastUserUpdated"] = user.get("name") if isinstance(user, dict) else None

    breadcrumb = parse_field(doc.get("breadcrumb", []))
    for item in breadcrumb if isinstance(breadcrumb, list) else []:
        tipo = item.get("type")
        if tipo in ("machine", "subset", "component", "spot"):
            row[tipo] = item.get("name")
            row[f"{tipo}Id"] = item.get("resourceId")
    return row


def etapa_laudos():
    dia = dia_referencia()
    inicio = datetime.combine(dia, dtime(0, 0, 0))
    fim = inicio + timedelta(days=1) - timedelta(seconds=1)

    docs = []
    for nome, context_id in WORKSPACES.items():
        # agora com paginação (antes só a 1ª página de 50 era lida)
        laudos = paginar_docs("/v1/technical-reports", {
            "contextId": context_id,
            "startAt": inicio.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "endAt": fim.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "sort": "reportedAt",
            "direction": -1,
            "limit": 50,
        })
        log.info("%s: %d laudos em %s", nome, len(laudos), dia)
        docs.extend(laudos)

    if not docs:
        log.info("Nenhum laudo novo")
        return pd.DataFrame()

    novos = pd.DataFrame([processar_laudo(d) for d in docs]).reindex(columns=COLUNAS_LAUDOS)
    novos = novos.drop_duplicates("reportId", keep="first").astype(object)

    aba = abrir_aba(PLANILHAS["laudos"])
    valores = sheets_retry(aba.get_all_values)
    if not valores or "reportId" not in valores[0]:
        gravar_aba(PLANILHAS["laudos"], novos, "laudos")
        return novos

    existente = pd.DataFrame(valores[1:], columns=valores[0])
    existente = existente.loc[:, ~existente.columns.duplicated()]
    existente = existente.drop_duplicates("reportId", keep="first").set_index("reportId", drop=False)
    novos_idx = novos.set_index("reportId", drop=False)

    colunas = list(existente.columns) + [c for c in novos.columns if c not in existente.columns]
    existente = existente.reindex(columns=colunas).astype(object)

    comuns = novos_idx.index.intersection(existente.index)
    inseridos = novos_idx.index.difference(existente.index)
    # Atualiza no lugar os laudos que já existiam (mesma posição na planilha)
    for rid in comuns:
        for col in novos_idx.columns:
            existente.at[rid, col] = novos_idx.at[rid, col]

    final = pd.concat([existente, novos_idx.loc[inseridos].reindex(columns=colunas)], ignore_index=True)
    gravar_aba(PLANILHAS["laudos"], final, "laudos")
    log.info("Laudos: %d novos, %d atualizados, %d no total", len(inseridos), len(comuns), len(final))
    return novos


# ---------------------------------------------------------------------------
# 6. TELEMETRIA (temperatura dos pontos das máquinas em MACHINELIST, D-1)
# ---------------------------------------------------------------------------

def etapa_telemetria(df_hier):
    spots = df_hier.loc[df_hier["machine.id"].isin(MACHINELIST), "spot.id"].dropna().unique().tolist()
    log.info("Spots para telemetria: %d", len(spots))
    if not spots:
        log.warning("Nenhum ponto encontrado para MACHINELIST=%s — verifique o ID da máquina", MACHINELIST)
        return pd.DataFrame()

    dia = dia_referencia()
    base_params = {
        "fromTime": dia.strftime("%Y-%m-%dT00:00:00Z"),
        "toTime": dia.strftime("%Y-%m-%dT23:59:59Z"),
    }

    registros = []
    for mp_id in spots:
        url, params = f"{API}/v1beta/telemetry/data-points/raw", {**base_params, "resourceId": mp_id}
        while url:
            data = api_get(url, params) or {}
            params = None  # a URL "next" já traz os parâmetros
            for item in data.get("data", []):
                nome = (item.get("displayName") or {}).get("pt")
                if not nome or "Temperatura" not in nome:
                    continue
                for dp in item.get("dataPoints", []):
                    registros.append({
                        "monitoringPointId": mp_id,
                        "attributes_axis": (item.get("attributes") or {}).get("axis"),
                        "displayName_pt": nome,
                        "unit": item.get("unit"),
                        "dataPoints_value": dp.get("value"),
                        "dataPoints_datetime": dp.get("datetime"),
                    })
            url = data.get("next")

    df = pd.DataFrame(registros)
    log.info("Telemetria: %d leituras", len(df))
    if df.empty:
        return df
    df = df.fillna("").astype(str)   # mesmo formato texto (RAW) de antes
    anexar_novos(
        PLANILHAS["telemetria"], df,
        chaves=["monitoringPointId", "attributes_axis", "displayName_pt", "unit",
                "dataPoints_value", "dataPoints_datetime"],
        nome="telemetria",
        value_input_option="RAW",
    )
    return df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    p = Pipeline("cda_online_dynamox")

    p.etapa("token", renovar_token)
    df_hier = p.etapa("hierarquia", etapa_hierarquia, depende_de=["token"])
    p.etapa("laudos", etapa_laudos, depende_de=["token"])
    p.etapa("telemetria", lambda: etapa_telemetria(df_hier), depende_de=["hierarquia"])
    p.etapa("alertas", lambda: etapa_alertas(df_hier), depende_de=["hierarquia"])

    p.finalizar()
