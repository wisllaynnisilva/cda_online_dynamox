# -*- coding: utf-8 -*-
"""cda_online_dynamox.ipynb

# **1. BIBLIOTECAS**
"""
from datetime import datetime, timedelta, timezone
from collections import deque
import pandas as pd
import requests
import time
import json
import jwt
import ast
import os

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

"""# **2. DADOS DE ACESSO**

## **2.1. Credenciais**
"""

# Salva o JSON em um arquivo
json_content = json.loads(os.environ["DYNAMOX_SERVICE_ACCOUNT"])

with open("application_key.json", "w") as json_file:
    json.dump(json_content, json_file)

"""## **2.2. URL's**"""

token_url = os.getenv("DYNAMOX_TOKEN")

"""## **2.3. Token**

### **2.3.1. Requisição de token**
"""

APPLICATION_KEY_PATH = "application_key.json"
URL = token_url

def generate_token(application_key_path: str) -> str:
    with open(application_key_path) as file:
        key = json.load(file)

    return jwt.encode(
      headers={
        "kid": key["_id"],
        "alg": "RS256",
        "typ": "JWT",
      },
      payload={
        "iat": datetime.now(timezone.utc).timestamp(),
        "email": key["email"],
      },
      key=key["privateKey"].encode("utf-8"),
    )

# Gera o token
token = generate_token(APPLICATION_KEY_PATH)
headers = {"Authorization": f"Bearer {token}"}

# Faz a requisição
response = requests.post(url=URL, headers=headers)
data = response.json()

"""### **2.3.2. Código token**"""

access_token = data["access_token"]
print(access_token)

"""## **2.4. Header**"""

header = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

"""## **2.5. Workspace**"""

workspace = {
    "ubu": os.getenv("DYNAMOX_UBU"),
    "germano": os.getenv("DYNAMOX_GERMANO")
}

"""## **2.6. Sheets**

### **2.6.1. Autenticação Sheets**
"""

# Lê a variável de ambiente com o conteúdo do JSON da conta de serviço
service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])

# Define os escopos de acesso (Google Sheets)
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Cria as credenciais usando o conteúdo do secret
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

# Autentica no Google Sheets
gc = gspread.authorize(creds)

"""# **3. HIERARQUIA**

## **3.1. Workspaces**

### **3.1.1. Execução**
"""

headers = header

# Lista filhos de um workspace (com paginação)
def list_children(parent_id):
    children = []
    page_token = None

    while True:
        params = {"parentId": parent_id, "limit": 100}
        if page_token:
            params["pageToken"] = page_token

        r = requests.get(
            "https://api.dynamox.solutions/v1/workspaces",
            headers=headers,
            params=params
        )

        if r.status_code != 200:
            print(f"Erro {r.status_code} para parentId={parent_id}")
            break

        data = r.json()
        children.extend(data.get("docs", []))

        page_token = data.get("items", {}).get("nextPageToken")
        if not page_token:
            break

    return children

# Desce a árvore recursivamente para cada workspace raiz
def traverse(ws_id, ws_name, ancestors=None):
    """
    Retorna lista de dicts com o caminho completo até cada folha.
    """
    if ancestors is None:
        ancestors = []

    current_path = ancestors + [{"id": ws_id, "name": ws_name}]
    children = list_children(ws_id)

    if not children:
        # Folha — nível mais próximo das machines
        return [current_path]

    results = []
    for child in children:
        results.extend(traverse(child["_id"], child["name"], current_path))
    return results

# Executa para cada workspace raiz
all_leaf_paths = {}

for site_name, root_id in workspace.items():
    print(f"\nPercorrendo {site_name} ({root_id})...")
    leaf_paths = traverse(root_id, site_name)
    all_leaf_paths[site_name] = leaf_paths
    print(f"  Folhas encontradas: {len(leaf_paths)}")

"""### **3.1.2. Estrutura e organização**"""

# Converte all_leaf_paths para DataFrame
rows = []

for site_name, leaf_paths in all_leaf_paths.items():
    for path in leaf_paths:
        # Gera uma linha para CADA nível do caminho
        for i in range(len(path)):
            row = {"site": site_name}
            # Preenche os níveis de 0 até i
            for level, ws in enumerate(path[:i+1]):
                row[f"workspace.{level}.id"]   = ws["id"]
                row[f"workspace.{level}.name"] = ws["name"]
            # last = o nível atual (i)
            row["last_workspaceId"]   = path[i]["id"]
            row["last_workspaceName"] = path[i]["name"]
            rows.append(row)

df_workspaces = pd.DataFrame(rows)

# Remove duplicatas — mesmo ws_id pode aparecer em vários caminhos
df_workspaces = df_workspaces.drop_duplicates(subset=["last_workspaceId"])

print(df_workspaces.shape)

"""## **3.2. Ativos**

### **3.2.1. Execução**
"""

headers = header

all_assets = []

for workspace_name, workspace_id in workspace.items():
    print(f"\nWorkspace: {workspace_name} ({workspace_id})")

    page = 1

    while True:
        url = f"https://api.dynamox.solutions/v1/workspaces/{workspace_id}/assets?page={page}"

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Erro {response.status_code}")
            break

        data = response.json()

        if not data or "docs" not in data:
            print("Sem dados ou estrutura inesperada")
            break

        docs = data.get("docs", [])

        if docs:
            for d in docs:
                d["workspaceId"] = workspace_id
                d["workspaceName"] = workspace_name
            all_assets.extend(docs)

        # paginação
        if not data.get("pages", {}).get("hasNext", False):
            break

        page += 1

print(f"\nTotal de assets coletados: {len(all_assets)}")

"""### **3.2.2. Estrutura e organização**"""

df_assets = pd.DataFrame(all_assets)

"""### **3.2.3. Lista de Assets Id**"""

asset_ids = df_assets.query("depthAsset == 0")["_id"].tolist()

"""## **3.3. Pontos**

### **3.3.1. Execução**
"""

headers = header

# Cria um df auxiliar só com os assets filtrados
df_assets_filtrado = df_assets[df_assets["_id"].isin(asset_ids)]

all_points = []

for asset_id, workspace_id, workspace_name in zip(
    df_assets_filtrado["_id"],
    df_assets_filtrado["workspaceId"],
    df_assets_filtrado["workspaceName"]
):

    page = 1

    while True:
        url = f"https://api.dynamox.solutions/v1/assets/{asset_id}/monitoring-points?page={page}&limit=100"

        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Erro {response.status_code} no asset {asset_id}")
            break

        data = response.json()

        docs = data.get("docs", [])

        if docs:
            for d in docs:
                d["assetId"] = asset_id
                d["workspaceId"] = workspace_id
                d["workspaceName"] = workspace_name

            all_points.extend(docs)

        if not data.get("pages", {}).get("hasNext", False):
            break

        page += 1

print(f"\nTotal de pontos coletados: {len(all_points)}")

"""### **3.3.2. Estrutura e organização**"""

df_points = pd.DataFrame(all_points)

"""## **3.3. DataFrame**"""

# Índices para lookup O(1)
at_idx = df_assets.set_index("_id").to_dict("index")
ws_idx = df_workspaces.set_index("last_workspaceId").to_dict("index")

ws_id_cols   = sorted([c for c in df_workspaces.columns if c.endswith(".id")   and c.startswith("workspace.")])
ws_name_cols = sorted([c for c in df_workspaces.columns if c.endswith(".name") and c.startswith("workspace.")])

def parse_sensors(sensors_raw):
    """Extrai x, y, z do campo sensors (string ou lista)."""
    try:
        if isinstance(sensors_raw, str):
            sensors_raw = ast.literal_eval(sensors_raw)
        if sensors_raw and len(sensors_raw) > 0:
            axes = sensors_raw[0].get("axesOrientation", {})
            return axes.get("x"), axes.get("y"), axes.get("z")
    except Exception:
        pass
    return None, None, None

def subir_hierarquia(point):
    row = {}

    # SPOT (ponto)
    row["spot.id"]        = point.get("_id")
    row["spot.name"]      = point.get("name")
    row["spot.createdAt"] = point.get("createdAt")
    row["spot.updatedAt"] = point.get("updatedAt")
    row["sensor_x"], row["sensor_y"], row["sensor_z"] = parse_sensors(point.get("sensors"))

    # SUBIR PELOS ATIVOS
    # Caso normal:  ponto.parentId → depth1 ou depth2 → depth0
    # Caso especial: ponto.parentId == ponto._id → vai direto pelo assetId (depth0)

    parent_id = point.get("parentId")
    asset_id  = point.get("assetId")

    # Detecta auto-referência (bug na plataforma)
    auto_ref = (parent_id == point.get("_id"))

    depth0 = depth1 = depth2 = None

    if auto_ref:
        # Vai direto para o depth0 via assetId
        depth0 = at_idx.get(asset_id)
    else:
        first_asset = at_idx.get(parent_id)
        if first_asset:
            d = str(first_asset.get("depthAsset", ""))
            if d == "2":
                depth2 = first_asset
                depth2["_id"] = parent_id
                depth1_id = first_asset.get("parentId")
                depth1 = at_idx.get(depth1_id)
                if depth1:
                    depth1["_id"] = depth1_id
                    depth0 = at_idx.get(depth1.get("parentId"))
            elif d == "1":
                depth1 = first_asset
                depth1["_id"] = parent_id
                depth0 = at_idx.get(first_asset.get("parentId"))
            elif d == "0":
                depth0 = first_asset
                depth0["_id"] = parent_id

    # Preenche subset (depth1 ou depth2, o mais próximo do spot)
    subset = depth2 if depth2 else depth1

    if subset:
        row["subset.id"]   = subset.get("_id") or parent_id
        row["subset.name"] = subset.get("name")

    if depth1 and depth2:
        # Tem os dois — guarda depth1 também separado se quiser
        row["component.id"]   = depth1.get("_id")
        row["component.name"] = depth1.get("name")

    # Machine = depth0
    if depth0:
        d0_id = depth0.get("_id") or (at_idx.get(asset_id, {}).get("_id") if auto_ref else None)
        row["machine.id"]              = asset_id if auto_ref else depth1.get("parentId") if depth1 else parent_id
        row["machine.name"]            = depth0.get("name")

        # WORKSPACES
        ws_parent = depth0.get("parentId")
        ws = ws_idx.get(ws_parent)
        if ws:
            row["last_workspaceId"]   = ws_parent
            row["last_workspaceName"] = ws.get("last_workspaceName")
            for col in ws_id_cols + ws_name_cols:
                row[col] = ws.get(col)

    return row

# Aplica para todos os pontos
rows = [subir_hierarquia(p) for p in df_points.to_dict("records")]
df_final = pd.DataFrame(rows)

# Ordena colunas
ws_cols = sorted([c for c in df_final.columns if c.startswith("workspace.")])
ordered = (
    ws_cols
    + ["last_workspaceId", "last_workspaceName"]
    + ["machine.id", "machine.name", "machine.configurationType"]
    + ["subset.id", "subset.name"]
    + ["component.id", "component.name"]
    + ["spot.id", "spot.name"]
    + ["sensor_x", "sensor_y", "sensor_z"]
    + ["spot.createdAt", "spot.updatedAt"]
)
ordered = [c for c in ordered if c in df_final.columns]
df_hierarquia = df_final[ordered]

print(f"Shape: {df_hierarquia.shape}")

# Verifica: quantos spots sem workspace (join falhou)?
sem_ws = df_hierarquia["workspace.0.id"].isna().sum()
print(f"Linhas sem workspace: {sem_ws}")
print()

"""## **3.4. Carga no Sheets**"""

# Nome da planilha (precisa já existir no seu Google Drive)
planilha_id = "1yh0_mkYbB-KbwUNwz9txY99bQHktD9mUFtDVscbOLag"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados (opcional)
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_hierarquia)

print("Dados enviados com sucesso para o Google Sheets!")

"""# **4. CONDIÇÃO**

## **4.1. Lista de pontos**
"""

spotsid = df_hierarquia["spot.id"].dropna().unique().tolist()

print(f"Total de spots: {len(spotsid)}")

"""## **4.2. Execução**"""

all_alert_policies = []

for spotid in spotsid:
    response = requests.get(
        "https://api.dynamox.solutions/v1/alert-policies/status",
        headers=headers,
        params={"resourceId": spotid}
    )

    if response.status_code == 200:
        policies = response.json()  # retorna lista direta
        for policy in policies:
            policy["spotId"] = spotid  # enriquece com o spot
        all_alert_policies.extend(policies)

    elif response.status_code == 429:
        wait = int(response.headers.get("Retry-After", 60))
        print(f"429 → esperando {wait}s")
        time.sleep(wait)
        # re-tenta o mesmo spot
        response = requests.get(
            "https://api.dynamox.solutions/v1/alert-policies/status",
            headers=headers,
            params={"resourceId": spotid}
        )
        if response.status_code == 200:
            policies = response.json()
            for policy in policies:
                policy["spotId"] = spotid
            all_alert_policies.extend(policies)

    else:
        print(f"Erro {response.status_code} no spot {spotid}")

    time.sleep(0.5)

df_alerts = pd.DataFrame(all_alert_policies)

"""## **4.3. DataFrame**"""

print(f"Total de policies: {len(df_alerts)}")

"""## **4.4. Carga no Sheets**"""

# Nome da planilha (precisa já existir no seu Google Drive)
planilha_id = "1DUmtzyd6WpsSYm9W5CWuyiQ_9-FkRbZz_luHhkH-CeI"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados (opcional)
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_alerts)

print("Dados enviados com sucesso para o Google Sheets!")

"""# **5. LAUDOS**

## **5.1. Execução**
"""

headers = header

workspaces = workspace

end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=1)

all_reports = []

for name, context_id in workspaces.items():

    print(f"\nWorkspace: {name}")

    params = {
        "contextId": context_id,
        "startAt": start_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "endAt": (end_date - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "sort": "reportedAt",
        "direction": -1,
        "limit": 50,
        "page": 1
    }

    response = requests.get(
        "https://api.dynamox.solutions/v1/technical-reports",
        headers=headers,
        params=params
    )

    print("Status:", response.status_code)

    if response.status_code == 200:
        data = response.json() or {}
        docs = data.get("docs") or []

        if not docs:
            print(f"{name}: sem laudos no período")
        else:
            print(f"{name}: {len(docs)} laudos coletados")
            all_reports.extend(docs)

    elif response.status_code == 429:
        wait = int(response.headers.get("Retry-After", 600))
        print(f"429 → esperando {wait}s")
        time.sleep(wait)

    else:
        print("Erro:", response.text)

    time.sleep(5)

print(f"\nTotal coletado: {len(all_reports)}")

"""## **5.2. Estrutura e organização**"""

def parse_field(val):
    """Converte string de dict/list para objeto Python."""
    if isinstance(val, str):
        try:
            return ast.literal_eval(val)
        except Exception:
            return val
    return val

def processar_laudo(doc):
    row = {
        "component": None,
        "componentId": None,
        "subset": None,
        "subsetId": None,
        "machine": None,
        "machineId": None,
        "spot": None,
        "spotId": None
    }

    # IDs e status
    row["reportId"]   = doc.get("_id")
    row["status"]     = doc.get("alertLevel")
    row["resolution"] = doc.get("status")
    row["type"]       = doc.get("type")
    row["noteNumber"] = doc.get("noteNumber")
    row["statusNote"] = doc.get("statusNote")
    row["deadline"]   = doc.get("deadline")
    row["criticality"]   = doc.get("criticality")
    row["deleted"] = doc.get("deleted")

    # Datas
    row["createdAt"] = doc.get("createdAt")
    row["reportedAt"] = doc.get("reportedAt")
    row["detected"]  = doc.get("detected")
    row["lastUserUpdateAt"] = doc.get("lastUserUpdateAt")
    row["updatedAt"] = doc.get("updatedAt")
    row["deletedAt"]   = doc.get("deletedAt")

    status_history = parse_field(doc.get("statusHistory", {}))
    if isinstance(status_history, dict):
        row["open"]       = status_history.get("open")
        row["inProgress"] = status_history.get("inProgress")
        row["closed"]     = status_history.get("closed")

    # Conteúdo técnico
    row["diagnostic"]   = doc.get("diagnostic")
    row["action"]       = doc.get("action")
    row["observation"]  = doc.get("note")

    # failures é lista → junta em string
    failures = parse_field(doc.get("failures", []))
    if isinstance(failures, list):
        # filtra 'none' e junta
        row["failures"] = ", ".join(f for f in failures if f != "none") or None
    else:
        row["failures"] = failures

    # Criador
    user = parse_field(doc.get("user", {}))
    row["userCreated"] = user.get("name") if isinstance(user, dict) else None

    # Modificador
    user = parse_field(doc.get("lastUpdatedBy", {}))
    row["lastUserUpdated"] = user.get("name") if isinstance(user, dict) else None

    # Hierarquia via breadcrumb
    # breadcrumb: [{"resourceId":..., "name":..., "type": machine|subset|spot}, ...]
    breadcrumb = parse_field(doc.get("breadcrumb", []))
    if isinstance(breadcrumb, list):
        for item in breadcrumb:
            t = item.get("type")
            if t == "machine":
                row["machine"]   = item.get("name")
                row["machineId"] = item.get("resourceId")
            elif t == "subset":
                row["subset"]   = item.get("name")
                row["subsetId"] = item.get("resourceId")
            elif t == "component":
                row["component"]   = item.get("name")
                row["componentId"] = item.get("resourceId")
            elif t == "spot":
                row["spot"]   = item.get("name")
                row["spotId"] = item.get("resourceId")

    return row

# Aplica para todos os laudos coletados
df_laudos = pd.DataFrame([processar_laudo(doc) for doc in all_reports])

# Ordena colunas
ordered = [
    "reportId", "reportedAt",
    "detected", "resolution", "status",
    "open", "inProgress", "closed",
    "machine", "machineId",
    "subset", "subsetId",
    "component", "componentId",
    "spot", "spotId",
    "diagnostic", "failures", "action",
    "noteNumber", "statusNote", "observation",
    "deadline", "criticality",
    "type", "userCreated", "createdAt",
    "lastUserUpdated", "lastUserUpdateAt",
    "deleted", "deletedAt", "updatedAt"
]
ordered = [c for c in ordered if c in df_laudos.columns]
df_laudos = df_laudos[ordered]

print(f"Shape: {df_laudos.shape}")

"""## **5.4. Carga no Sheets**"""

# Abre a planilha
planilha_id = "1x6AJxILgPxb13LCaK1L8qizZaxGK3qOZtTiR_BwqyoA"
nome_da_aba      = "Sheet1"

planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Verifica se df_laudos está vazio
if df_laudos.empty:
    print("Nenhum dado novo para processar.")
else:
    # Lê o que já existe na planilha
    dados_existentes = aba.get_all_records()
    df_existente     = pd.DataFrame(dados_existentes)

    if df_existente.empty:
        # Planilha vazia → escreve tudo
        set_with_dataframe(aba, df_laudos)
        print(f"Planilha vazia. {len(df_laudos)} registros inseridos.")

    else:
        novos      = []
        atualizados = 0

        for _, row in df_laudos.iterrows():
            report_id = row["reportId"]
            match     = df_existente[df_existente["reportId"] == report_id]

            if match.empty:
                # ID novo → adiciona
                novos.append(row)

            else:
                # ID existente → compara lastUserUpdateAt
                idx_planilha    = match.index[0]
                update_existente = str(match.iloc[0].get("lastUserUpdateAt", ""))
                update_novo      = str(row.get("lastUserUpdateAt", ""))

                if update_novo != update_existente:
                    # Atualizado → substitui a linha inteira
                    df_existente.loc[idx_planilha] = row
                    atualizados += 1
                # else: duplicata, ignora

        # Adiciona os novos ao df_existente
        if novos:
            df_existente = pd.concat([df_existente, pd.DataFrame(novos)], ignore_index=True)

        # Reescreve a planilha com tudo atualizado
        aba.clear()
        set_with_dataframe(aba, df_existente)

        print(f"Novos inseridos: {len(novos)}")
        print(f"Atualizados: {atualizados}")
        print(f"Total na planilha: {len(df_existente)}")

"""# **6. TELEMETRIA**

## **6.1. Pontos**

### **6.1.1. Lista de ativos**
"""

machinelist = ['6890aaa1f3702a4bac37381f', '6890aabdf17babfd7b238b99', '6862af0b4a0902176cf31759', '679785193b33cc5cb6737df3', '6797851f9960697841be5afa', '67a0b263e9e2c6fb27d25f4e', '6862af3ec10fddfc64c4db8d', '6862af51c10fddfc64c4db8e', '6797851ca0e078f5eb9d6ff5', '6862af2c135084dab506fca9', '681a0e3da612598ea8e88677', '65fad3f8155bf676add56ade', '65f4609849d1581201e7c587', '6604254ce5bf4746a9abb972', '65eb0b9846f4172a16199ead', '6862aeb74a0902176cf31758', '6862aea0135084dab506fca8', '6862ae849b05c5c1126bca4a', '6862aee31d45e006fe05ab54']

"""### **6.1.2. Lista de pontos**"""

spotlist = df_hierarquia[df_hierarquia["machine.id"].isin(machinelist)]["spot.id"].tolist()

print(f"Spots: {len(set(spotlist))}")
print(spotlist)

"""## **6.2. Medições**

### **6.2.1. Execução**
"""

API_URL = "https://api.dynamox.solutions/v1beta/telemetry/data-points/raw"

headers = header

UTC = timezone.utc
hoje = datetime.now(timezone.utc)
ontem = hoje - timedelta(days=1)

# Ontem 00:00 até ontem 23:59:59
from_time = ontem.strftime("%Y-%m-%dT00:00:00Z")
to_time   = ontem.strftime("%Y-%m-%dT23:59:59Z")

# Lista final
telemetry_docs = []

for mp_id in spotlist:
    params = {
        "resourceId": mp_id,
        "fromTime": from_time,
        "toTime": to_time
    }

    url = API_URL
    while url:  # loop para paginação
        response = requests.get(url, headers=headers, params=params)
        params = None  # só manda params na primeira requisição

        if response.status_code == 200:
            data = response.json()

            # Extrair só os campos necessários
            for item in data.get("data", []):
                displayName_pt = item.get("displayName", {}).get("pt")
                attributes_axis = item.get("attributes", {}).get("axis")
                unit = item.get("unit")

                for dp in item.get("dataPoints", []):
                    if displayName_pt and "Temperatura" in displayName_pt:
                        telemetry_docs.append({
                            "monitoringPointId": mp_id,
                            "displayName_pt": displayName_pt,
                            "attributes_axis": attributes_axis,
                            "unit": unit,
                            "dataPoints_datetime": dp.get("datetime"),
                            "dataPoints_value": dp.get("value"),
                        })

            # Verifica se há próxima página
            url = data.get("next")
        else:
            print(f"Erro para o ID {mp_id}: {response.status_code} - {response.text}")
            break

# Exibir resultado final
print(json.dumps(telemetry_docs, indent=2, ensure_ascii=False))

"""### **6.2.2. Estrutura e organização**"""

df_telemetry = pd.DataFrame(telemetry_docs)

# Lista com os nomes das colunas que você quer manter
colunas_desejadas = ["monitoringPointId", "attributes_axis", "displayName_pt", "unit", "dataPoints_value", "dataPoints_datetime"]

# Verifica se o DataFrame não está vazio e contém as colunas desejadas
if not df_telemetry.empty and all(col in df_telemetry.columns for col in colunas_desejadas):
    df_telemetry = df_telemetry[colunas_desejadas]
    display(df_telemetry)
else:
    print("DataFrame vazio ou colunas desejadas não estão presentes.")
    df_telemetry = pd.DataFrame(columns=colunas_desejadas)

"""### **6.2.4. Carga no Sheets**"""

# Nome da planilha e aba
planilha_id = "11rzDwr5U6HAfIUXHLNOCcoFkmPEcOUNDpTnZ4Nmg2uA"
nome_da_aba = "Sheet1"

# Abre a planilha e a aba
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Lê os valores atuais da aba
dados_existentes = aba.get_all_values()

# Converte para DataFrame (se houver dados)
if dados_existentes:
    df_existente = pd.DataFrame(dados_existentes[1:], columns=dados_existentes[0])  # ignora cabeçalho
else:
    df_existente = pd.DataFrame()

# Prepara o novo DataFrame (df_telemetry precisa já estar criado antes)
df_novo = df_telemetry.copy()

# Converte colunas complexas (dicionários, listas) para string
for col in df_novo.select_dtypes(include=["object"]).columns:
    df_novo[col] = df_novo[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

# Define as colunas para comparação de duplicados
chaves = ["monitoringPointId", "attributes_axis", "displayName_pt", "unit", "dataPoints_value", "dataPoints_datetime"]

# Se já houver dados existentes, remover duplicados
if not df_existente.empty:
    # Garante que todas as colunas existem nos dois DataFrames
    colunas_comuns = [c for c in chaves if c in df_existente.columns and c in df_novo.columns]
    # Cria um conjunto de tuplas com as combinações já existentes
    existentes_set = set(df_existente[colunas_comuns].apply(tuple, axis=1).tolist())
    # Filtra os novos dados mantendo apenas combinações não existentes
    df_filtrado = df_novo[~df_novo[colunas_comuns].apply(tuple, axis=1).isin(existentes_set)]
else:
    df_filtrado = df_novo.copy()

# Se for a primeira vez, adiciona os cabeçalhos
if not dados_existentes:
    aba.append_row(df_filtrado.columns.tolist())

# Adiciona somente os dados novos
if not df_filtrado.empty:
    aba.append_rows(df_filtrado.values.tolist(), value_input_option="RAW")
    print(f"{len(df_filtrado)} novas linhas adicionadas com sucesso.")
else:
    print("Nenhuma nova linha para adicionar. Tudo já está na planilha.")

"""### **6.2.5. Remoção de Duplicadas**"""

planilha_id = "11rzDwr5U6HAfIUXHLNOCcoFkmPEcOUNDpTnZ4Nmg2uA"
nome_da_aba = "Sheet1"
chaves = ["monitoringPointId", "attributes_axis", "displayName_pt", "unit", "dataPoints_value", "dataPoints_datetime"]

planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)
dados_existentes = aba.get_all_values()

if not dados_existentes:
    print("A planilha está vazia. Nada a verificar.")
else:
    # Converte para DataFrame
    df = pd.DataFrame(dados_existentes[1:], columns=dados_existentes[0])
    linhas_antes = len(df)

    # Remove duplicados (mantém a primeira ocorrência)
    df_sem_dup = df.drop_duplicates(subset=chaves, keep="first")
    linhas_depois = len(df_sem_dup)

    # Se houve duplicação
    if linhas_depois < linhas_antes:
        # Limpa a planilha
        aba.clear()
        # Reescreve cabeçalho
        aba.append_row(df_sem_dup.columns.tolist())
        # Reescreve dados limpos
        aba.append_rows(df_sem_dup.values.tolist(), value_input_option="RAW")
        print(f"Removidas {linhas_antes - linhas_depois} linhas duplicadas. Planilha atualizada com sucesso.")
    else:
        print("Nenhuma duplicação encontrada. Tudo limpo.")

FIM
```
"""
