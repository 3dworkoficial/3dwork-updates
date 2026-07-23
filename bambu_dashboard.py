#!/usr/bin/env python3
"""
Bambu Lab — Painel de telemetria ao vivo
========================================

Assina o MQTT de uma ou mais impressoras Bambu Lab e serve um dashboard web
em tempo real (via WebSocket). Funciona em dois modos, por impressora:

  - "cloud": conecta no broker da nuvem da Bambu (us.mqtt.bambulab.com).
             Roda em qualquer lugar — um VPS, por exemplo — SEM precisar de
             nada na sua rede local. A impressora precisa estar em modo nuvem
             (NÃO "LAN only"). Use o bambu_login.py para obter uid + token.

  - "lan":   conecta direto no broker dentro da impressora. Exige estar na
             mesma rede local.

Como usar
---------
1. pip install paho-mqtt fastapi uvicorn
2. (modo nuvem) python bambu_login.py  ->  copie o uid e o token.
3. Edite o `printers.json` (criado na 1a execução) com as suas impressoras.
4. python bambu_dashboard.py
5. Abra http://localhost:8000 (ou o IP/porta do seu VPS).

Onde achar cada dado:
- serial: painel da impressora ou app Bambu Handy.
- (lan) ip + access_code: painel -> Configurações -> Geral -> "LAN only".
- (cloud) uid + token: rode o bambu_login.py.
"""

import asyncio
import base64
import json
import os
import secrets
import hashlib
import hmac
import ssl
import threading
import time
import random
import string
import re
import uuid
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM as _AESGCM_CHECK
    _HAS_CRYPTOGRAPHY = True
except ImportError:
    _HAS_CRYPTOGRAPHY = False
    print("[aviso] 'cryptography' não instalado — detecção de cookie do Chrome pode falhar.")

import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse, FileResponse
import uvicorn

# Versão do sistema — atualize aqui a cada nova entrega
APP_VERSION = "1.0"

# URL do arquivo que informa a versão mais recente publicada (GitHub raw).
# Troque "SEU_USUARIO/SEU_REPO" pelo seu repositório quando publicar.
UPDATE_INFO_URL = "https://raw.githubusercontent.com/3dworkoficial/3dwork-updates/main/versao.json"

CONFIG_PATH = Path(__file__).with_name("printers.json")

EXAMPLE_CONFIG = [
    {
        # MODO NUVEM: roda em qualquer lugar (VPS, etc). Não precisa de nada
        # na sua rede local. A impressora precisa estar em modo nuvem (NÃO
        # "LAN only"). Pegue uid + token com o bambu_login.py.
        "name": "X1 Carbon",
        "mode": "cloud",
        "serial": "00M00A0000000000",
        "region": "us",
        "uid": "u_1234567",
        "token": "COLE_O_CLOUD_ACCESS_TOKEN_AQUI",
    },
    {
        # MODO LOCAL: precisa estar na mesma rede da impressora.
        "name": "P1S Bancada",
        "mode": "lan",
        "ip": "192.168.1.51",
        "serial": "00M00B0000000000",
        "access_code": "87654321",
    },
]


def load_printers():
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text("[]")
        return []
    try:
        return json.loads(CONFIG_PATH.read_text())
    except Exception as exc:
        print("[config] printers.json inválido:", exc)
        return []


def save_printers(cfgs):
    CONFIG_PATH.write_text(json.dumps(cfgs, indent=2, ensure_ascii=False))


# Fonte da verdade em memória. A ordem da lista = ordem de exibição.
PRINTERS_CFG = []            # lista de configs
PRINTERS = {}               # nome -> {client, stop, thread}
ORDER = []                  # nomes na ordem de exibição
CFG_LOCK = threading.Lock()


def _sync_order():
    ORDER[:] = [c["name"] for c in PRINTERS_CFG]


def add_printer_cfg(cfg):
    name = cfg.get("name")
    if not name or not cfg.get("serial"):
        return False, "Faltam nome ou número de série."
    with CFG_LOCK:
        if any(c["name"] == name for c in PRINTERS_CFG):
            return False, "Já existe uma impressora com esse nome."
        PRINTERS_CFG.append(cfg)
        save_printers(PRINTERS_CFG)
        _sync_order()
    start_printer(cfg)
    return True, None


def remove_printer_cfg(name):
    with CFG_LOCK:
        idx = next((i for i, c in enumerate(PRINTERS_CFG) if c["name"] == name), None)
        if idx is None:
            return False, "Impressora não encontrada."
        PRINTERS_CFG.pop(idx)
        save_printers(PRINTERS_CFG)
        _sync_order()
    stop_printer(name)
    with STATE_LOCK:
        STATE.pop(name, None)
    broadcaster.notify_from_thread()
    return True, None


def reorder_printers(names):
    with CFG_LOCK:
        by = {c["name"]: c for c in PRINTERS_CFG}
        if set(names) != set(by):
            return False, "Lista de ordem inválida."
        PRINTERS_CFG[:] = [by[n] for n in names]
        save_printers(PRINTERS_CFG)
        _sync_order()
    broadcaster.notify_from_thread()
    return True, None


def stop_printer(name):
    handle = PRINTERS.pop(name, None)
    if not handle:
        return
    handle["stop"].set()
    client = handle.get("client")
    # Bambu: client é o próprio mqtt.Client. Anycubic: é um dict holder.
    if isinstance(client, dict):
        client = client.get("client")
    try:
        if client:
            client.disconnect()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Estado compartilhado entre as threads MQTT e o servidor web
# ---------------------------------------------------------------------------
STATE = {}          # nome_da_impressora -> dict de estado mesclado
STATE_LOCK = threading.Lock()


def deep_merge(base: dict, patch: dict) -> dict:
    """Mescla atualizações parciais (a P1 só envia o que mudou)."""
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


class Broadcaster:
    """Empurra o estado atual para todos os navegadores conectados."""

    def __init__(self):
        self.loop = None
        self.clients: set[WebSocket] = set()

    def snapshot(self) -> str:
        with STATE_LOCK:
            return json.dumps({"printers": STATE, "order": list(ORDER),
                               "costs": PRINT_COSTS})

    def notify_from_thread(self):
        """Chamado pela thread do MQTT quando chega dado novo."""
        if self.loop is None:
            return
        asyncio.run_coroutine_threadsafe(self._broadcast(), self.loop)

    async def _broadcast(self):
        if not self.clients:
            return
        payload = self.snapshot()
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


broadcaster = Broadcaster()


# ---------------------------------------------------------------------------
# Histórico no Supabase (opcional). Crie um supabase.json ao lado do script:
#   {"url": "https://xxxx.supabase.co", "key": "SUA_CHAVE", "table": "print_jobs"}
# Sem esse arquivo, o histórico fica desligado e o resto funciona igual.
# ---------------------------------------------------------------------------
class SupabaseLogger:
    def __init__(self):
        self.url = None
        self.key = None
        self.table = "print_jobs"
        self.enabled = False
        self._last_state = {}   # nome -> último gcode_state
        self._start_ts = {}     # nome -> epoch de início da impressão
        self._load()

    def _load(self):
        path = Path(__file__).with_name("supabase.json")
        if not path.exists():
            print("[supabase] sem supabase.json — histórico desligado.")
            return
        try:
            cfg = json.loads(path.read_text())
            self.url = cfg.get("url", "").rstrip("/")
            self.key = cfg.get("key", "")
            self.table = cfg.get("table", "print_jobs")
            self.enabled = bool(self.url and self.key and requests)
            if self.enabled:
                print(f"[supabase] histórico ativo -> tabela {self.table}")
            else:
                print("[supabase] url/key faltando (ou requests não instalado).")
        except Exception as exc:
            print("[supabase] config inválida:", exc)

    def _headers(self):
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
        }

    def observe(self, name, p):
        """Detecta início e término de impressão e registra cada job concluído."""
        if not self.enabled:
            return
        state = p.get("gcode_state")
        if not state:
            return
        prev = self._last_state.get(name)
        self._last_state[name] = state
        printing = ("RUNNING", "PREPARE")
        if state in printing and prev not in ("RUNNING", "PREPARE", "PAUSE"):
            self._start_ts[name] = time.time()
        if state in ("FINISH", "FAILED") and prev in ("RUNNING", "PREPARE", "PAUSE"):
            self._log_job(name, p, state)

    def _log_job(self, name, p, state):
        start = self._start_ts.pop(name, None)
        duration = int(time.time() - start) if start else None
        file_name = (p.get("subtask_name") or p.get("gcode_file") or "").split("/")[-1]
        row = {
            "printer": name,
            "file": file_name,
            "result": "success" if state == "FINISH" else "failed",
            "layers": p.get("layer_num"),
            "total_layers": p.get("total_layer_num"),
            "duration_sec": duration,
            "error_code": (p.get("print_error") or None),
        }
        try:
            r = requests.post(
                f"{self.url}/rest/v1/{self.table}",
                headers={**self._headers(), "Prefer": "return=minimal"},
                json=row, timeout=10,
            )
            if r.status_code < 400:
                print(f"[supabase] job registrado: {name} -> {row['result']}")
            else:
                print(f"[supabase] erro {r.status_code}: {r.text[:200]}")
        except Exception as exc:
            print("[supabase] falha ao registrar:", exc)

    def stats(self):
        if not self.enabled:
            return {"enabled": False}
        try:
            since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
            r = requests.get(
                f"{self.url}/rest/v1/{self.table}",
                headers=self._headers(),
                params={
                    "select": "printer,result,duration_sec,finished_at",
                    "finished_at": f"gte.{since}",
                    "order": "finished_at.desc",
                    "limit": "500",
                },
                timeout=10,
            )
            jobs = r.json() if r.status_code < 400 else []
        except Exception:
            jobs = []
        today = datetime.now().date().isoformat()
        total = len(jobs)
        success = sum(1 for j in jobs if j.get("result") == "success")
        today_jobs = sum(1 for j in jobs if str(j.get("finished_at", ""))[:10] == today)
        dur = sum(j.get("duration_sec") or 0 for j in jobs)
        return {
            "enabled": True,
            "week_jobs": total,
            "today_jobs": today_jobs,
            "success_rate": round(success / total * 100) if total else None,
            "print_hours": round(dur / 3600, 1),
        }


supabase_logger = SupabaseLogger()


# ===========================================================================
# LICENÇA — a lógica fica em hub3d_core (compilado com Nuitka → .pyd)
# Se o módulo compilado não existir, o sistema não valida licença e bloqueia.
# ===========================================================================
try:
    import hub3d_core as _core
    machine_fingerprint = _core.machine_fingerprint
    check_license = _core.check_license
    install_license = _core.install_license
    _CORE_OK = True
except Exception as _e:
    print(f"[licenca] núcleo de licença ausente ou inválido: {_e}")
    _CORE_OK = False

    def machine_fingerprint():
        return "NUCLEO-AUSENTE"

    def check_license():
        return {"ok": False, "reason": "nucleo_ausente",
                "fingerprint": "NUCLEO-AUSENTE"}

    def install_license(key_text):
        return False, "Núcleo de licença ausente. Reinstale o sistema."


# Estado global da licença (checado no startup)
LICENSE_STATE = {"ok": False, "fingerprint": "", "reason": "sem_licenca"}


def refresh_license():
    global LICENSE_STATE
    LICENSE_STATE = check_license()
    return LICENSE_STATE


# ---------------------------------------------------------------------------
# Histórico local em SQLite (sempre ativo, sem configuração).
# Registra cada impressão concluída para os relatórios.
# ---------------------------------------------------------------------------
import sqlite3


# ---------------------------------------------------------------------------
# Custo por impressão — informado pelo usuário quando uma impressão começa.
# Guardado em disco para sobreviver a reinícios enquanto a peça imprime.
# ---------------------------------------------------------------------------
COSTS_PATH = Path(__file__).with_name("custos_ativos.json")
PRINT_COSTS = {}          # nome da impressora -> dados do custo
COSTS_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Gerenciador de Projetos (STL, 3MF, OBJ, G-code)
# Dois locais: "local" (dentro da pasta do sistema) e "nuvem" (uma pasta que
# o cliente sincroniza com Google Drive/OneDrive/etc — configurável).
# ---------------------------------------------------------------------------
import shutil as _shutil

PROJ_CONFIG_PATH = Path(__file__).with_name("projetos_config.json")
PROJ_LOCAL_DIR = Path(__file__).with_name("projetos")           # local padrão
PROJ_EXTS = {".stl", ".3mf", ".obj", ".gcode", ".g", ".gco"}
PROJ_MAX_MB = 200


def _proj_cfg():
    """Lê a config: onde fica a pasta 'nuvem' do cliente."""
    try:
        if PROJ_CONFIG_PATH.exists():
            return json.loads(PROJ_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"cloud_dir": ""}


def _proj_save_cfg(cfg):
    try:
        PROJ_CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2),
                                    encoding="utf-8")
    except Exception:
        pass


def _proj_root(local):
    """Raiz de cada local de armazenamento."""
    if local == "nuvem":
        cd = _proj_cfg().get("cloud_dir", "")
        if cd and Path(cd).exists():
            return Path(cd)
        return None
    PROJ_LOCAL_DIR.mkdir(exist_ok=True)
    return PROJ_LOCAL_DIR


def _proj_safe(root, rel):
    """Resolve um caminho relativo garantindo que fique dentro da raiz."""
    if root is None:
        return None
    rel = (rel or "").strip().lstrip("/\\")
    target = (root / rel).resolve()
    try:
        target.relative_to(root.resolve())
    except ValueError:
        return None            # tentativa de escapar da pasta (../)
    return target


def _fmt_size(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# --- Abertura de arquivos no fatiador (slicer) ------------------------------
# Cada marca tem seu programa. O sistema procura o executável nos caminhos
# padrão de instalação no Windows. Caminhos personalizados podem ser salvos.
SLICER_CONFIG_PATH = Path(__file__).with_name("slicers_config.json")

SLICER_DEFAULTS = {
    "bambu": {
        "nome": "Bambu Studio",
        "paths": [
            r"C:\Program Files\Bambu Studio\bambu-studio.exe",
            r"C:\Program Files (x86)\Bambu Studio\bambu-studio.exe",
        ],
    },
    "anycubic": {
        "nome": "Anycubic Slicer",
        "paths": [
            r"C:\Program Files\Anycubic Slicer Next\AnycubicSlicerNext.exe",
            r"C:\Program Files\AnycubicSlicer\AnycubicSlicer.exe",
            r"C:\Program Files\Anycubic Slicer\AnycubicSlicer.exe",
            r"C:\Program Files (x86)\Anycubic Slicer\AnycubicSlicer.exe",
        ],
    },
}


def _slicer_cfg():
    try:
        if SLICER_CONFIG_PATH.exists():
            return json.loads(SLICER_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _slicer_save_cfg(cfg):
    try:
        SLICER_CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
    except Exception:
        pass


def _find_slicer(brand):
    """Retorna o caminho do executável do fatiador da marca, ou None."""
    # 1) caminho salvo pelo usuário tem prioridade
    saved = _slicer_cfg().get(brand)
    if saved and Path(saved).exists():
        return saved
    # 2) caminhos padrão de instalação
    for p in SLICER_DEFAULTS.get(brand, {}).get("paths", []):
        if Path(p).exists():
            return p
    return None


def _open_in_slicer(brand, file_path):
    """Abre o arquivo no fatiador da marca. Retorna (ok, erro)."""
    exe = _find_slicer(brand)
    nome = SLICER_DEFAULTS.get(brand, {}).get("nome", "o fatiador")
    if not exe:
        return False, f"{nome} não foi encontrado neste computador."
    if not Path(file_path).exists():
        return False, "Arquivo não encontrado."
    try:
        import subprocess
        # abre o programa com o arquivo como argumento (não bloqueia o servidor)
        subprocess.Popen([exe, str(file_path)],
                         creationflags=getattr(subprocess, "DETACHED_PROCESS", 0))
        return True, None
    except Exception as exc:
        return False, f"Não foi possível abrir: {exc}"


def proj_list(local, rel):
    root = _proj_root(local)
    if root is None:
        return {"ok": False, "error": "cloud_nao_configurada"}
    base = _proj_safe(root, rel)
    if base is None or not base.exists():
        base = root
    folders, files = [], []
    try:
        for item in sorted(base.iterdir(), key=lambda x: x.name.lower()):
            if item.name.startswith("."):
                continue
            r = str(item.relative_to(root)).replace("\\", "/")
            if item.is_dir():
                folders.append({"name": item.name, "rel": r})
            elif item.suffix.lower() in PROJ_EXTS:
                st = item.stat()
                files.append({"name": item.name, "rel": r,
                              "size": _fmt_size(st.st_size),
                              "ext": item.suffix.lower().lstrip("."),
                              "mtime": int(st.st_mtime)})
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "folders": folders, "files": files,
            "path": rel or "", "local": local}



def _load_print_costs():
    global PRINT_COSTS
    try:
        if COSTS_PATH.exists():
            PRINT_COSTS = json.loads(COSTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        PRINT_COSTS = {}


def _save_print_costs():
    try:
        COSTS_PATH.write_text(json.dumps(PRINT_COSTS, ensure_ascii=False,
                                         indent=2), encoding="utf-8")
    except Exception:
        pass


def set_print_cost(name, data):
    with COSTS_LOCK:
        PRINT_COSTS[name] = data
        _save_print_costs()


def clear_print_cost(name):
    with COSTS_LOCK:
        if name in PRINT_COSTS:
            PRINT_COSTS.pop(name, None)
            _save_print_costs()


def calcular_custo(peso_g, preco_kg, minutos, preco_kwh, potencia_w=150.0):
    """Custo de uma impressão: material + energia."""
    try:
        material = (float(peso_g) / 1000.0) * float(preco_kg)
        horas = float(minutos) / 60.0
        energia = (float(potencia_w) / 1000.0) * horas * float(preco_kwh)
        return round(material + energia, 2), round(material, 2), round(energia, 2)
    except (TypeError, ValueError):
        return 0.0, 0.0, 0.0


_load_print_costs()


class HistoryDB:
    def __init__(self):
        self.path = str(Path(__file__).with_name("historico.db"))
        self._lock = threading.Lock()
        self._last_state = {}
        self._start_ts = {}
        self._last_pct = {}     # último progresso visto (para detectar fim)
        self._last_obj = {}     # último nome de objeto visto
        self._last_cfg = {}     # último arquivo de configuração visto
        self._init_db()

    def _init_db(self):
        try:
            con = sqlite3.connect(self.path)
            con.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    printer TEXT NOT NULL,
                    brand TEXT,
                    file TEXT,
                    result TEXT,
                    layers INTEGER,
                    total_layers INTEGER,
                    duration_sec INTEGER,
                    started_at TEXT,
                    finished_at TEXT
                )
            """)
            # Migração: colunas de custo (bancos antigos não têm)
            cols = {r[1] for r in con.execute("PRAGMA table_info(jobs)")}
            for col, tipo in (("custo", "REAL"), ("peso_g", "REAL"),
                              ("material", "TEXT"), ("config", "TEXT")):
                if col not in cols:
                    con.execute(f"ALTER TABLE jobs ADD COLUMN {col} {tipo}")
            con.commit()
            con.close()
            print(f"[historico] banco local em {self.path}")
        except Exception as exc:
            print(f"[historico] erro ao criar banco: {exc}")

    def observe(self, name, p, brand=None):
        """Detecta início e fim de impressão e registra jobs concluídos."""
        state = p.get("gcode_state")
        if not state:
            return
        prev = self._last_state.get(name)
        self._last_state[name] = state
        printing = ("RUNNING", "PREPARE")
        ativo = ("RUNNING", "PREPARE", "PAUSE")

        if state in printing and prev not in ativo:
            self._start_ts[name] = time.time()

        # guarda o último progresso visto enquanto imprime (usado abaixo)
        if state in ativo:
            pct = p.get("mc_percent")
            if pct is not None:
                self._last_pct[name] = pct
            # guarda o nome do objeto enquanto imprime — algumas impressoras
            # limpam esse campo assim que terminam
            obj = p.get("subtask_name")
            if obj:
                self._last_obj[name] = obj
            cfgf = p.get("gcode_file")
            if cfgf:
                self._last_cfg[name] = cfgf

        if prev in ativo:
            if state in ("FINISH", "FAILED"):
                self._log_job(name, p, state, brand)
            elif state == "IDLE" and self._start_ts.get(name):
                # Muitas Anycubic voltam para "livre/idle" ao terminar, sem
                # passar por "finished". Usa o último progresso para decidir.
                pct = self._last_pct.get(name) or 0
                resultado = "FINISH" if pct >= 95 else "FAILED"
                self._log_job(name, p, resultado, brand)

    def _log_job(self, name, p, state, brand):
        start = self._start_ts.pop(name, None)
        now = time.time()
        duration = int(now - start) if start else None
        # Nome do OBJETO (o que o usuário fatiou). O gcode_file costuma ser a
        # configuração da mesa (ex.: plate_1.gcode), por isso vai em separado.
        obj = p.get("subtask_name") or self._last_obj.get(name) or ""
        file_name = str(obj).split("/")[-1]
        cfg_raw = p.get("gcode_file") or self._last_cfg.get(name) or ""
        config = str(cfg_raw).split("/")[-1]
        if not file_name:                     # sem nome do objeto: usa o que houver
            file_name = config
            config = ""
        self._last_pct.pop(name, None)
        self._last_obj.pop(name, None)
        self._last_cfg.pop(name, None)
        started_iso = (datetime.fromtimestamp(start, timezone.utc).isoformat()
                       if start else None)
        finished_iso = datetime.fromtimestamp(now, timezone.utc).isoformat()
        try:
            # custo informado pelo usuário para esta impressão (se houver)
            info = PRINT_COSTS.get(name) or {}
            custo = info.get("custo")
            peso_g = info.get("peso_g")
            material = info.get("material")
            with self._lock:
                con = sqlite3.connect(self.path)
                con.execute(
                    "INSERT INTO jobs (printer,brand,file,result,layers,"
                    "total_layers,duration_sec,started_at,finished_at,"
                    "custo,peso_g,material,config) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (name, brand, file_name,
                     "success" if state == "FINISH" else "failed",
                     p.get("layer_num"), p.get("total_layer_num"),
                     duration, started_iso, finished_iso,
                     custo, peso_g, material, config))
                con.commit()
                con.close()
            print(f"[historico] job registrado: {name} -> {state}"
                  + (f" (custo R$ {custo:.2f})" if custo else ""))
        except Exception as exc:
            print(f"[historico] erro ao registrar: {exc}")
        # limpa o custo desta impressão (a próxima será perguntada de novo)
        clear_print_cost(name)

    def manual_add(self, printer, brand, file, result, duration_sec,
                   started_at, finished_at, layers=None, total_layers=None):
        """Insere um registro manual (para testes/importação)."""
        try:
            with self._lock:
                con = sqlite3.connect(self.path)
                con.execute(
                    "INSERT INTO jobs (printer,brand,file,result,layers,"
                    "total_layers,duration_sec,started_at,finished_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (printer, brand, file, result, layers, total_layers,
                     duration_sec, started_at, finished_at))
                con.commit()
                con.close()
        except Exception as exc:
            print(f"[historico] erro manual_add: {exc}")

    def query(self, start_iso=None, end_iso=None, printers=None):
        """Retorna jobs no período, opcionalmente filtrados por impressora."""
        try:
            with self._lock:
                con = sqlite3.connect(self.path)
                con.row_factory = sqlite3.Row
                sql = "SELECT * FROM jobs WHERE 1=1"
                args = []
                if start_iso:
                    sql += " AND finished_at >= ?"
                    args.append(start_iso)
                if end_iso:
                    sql += " AND finished_at <= ?"
                    args.append(end_iso)
                if printers:
                    ph = ",".join("?" * len(printers))
                    sql += f" AND printer IN ({ph})"
                    args.extend(printers)
                sql += " ORDER BY finished_at DESC"
                rows = [dict(r) for r in con.execute(sql, args).fetchall()]
                con.close()
                return rows
        except Exception as exc:
            print(f"[historico] erro na consulta: {exc}")
            return []

    def all_printers(self):
        """Lista as impressoras que têm histórico."""
        try:
            with self._lock:
                con = sqlite3.connect(self.path)
                rows = con.execute(
                    "SELECT DISTINCT printer FROM jobs ORDER BY printer").fetchall()
                con.close()
                return [r[0] for r in rows]
        except Exception:
            return []


history_db = HistoryDB()


# ---------------------------------------------------------------------------
# Autenticação (login por usuário e senha). Guarda credenciais em auth.json.
# Na primeira execução cria um usuário "admin" com senha aleatória e a mostra
# no console. Você pode trocar a senha depois, dentro do painel.
# ---------------------------------------------------------------------------
AUTH_PATH = Path(__file__).with_name("auth.json")
SESSION_COOKIE = "farm_session"
SESSION_TTL = 7 * 24 * 3600          # 7 dias
_SESSIONS = {}                        # sid -> expiração (epoch)


def _hash_pw(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200_000).hex()


def load_or_create_auth():
    if AUTH_PATH.exists():
        try:
            return json.loads(AUTH_PATH.read_text())
        except Exception as exc:
            print("[auth] auth.json inválido:", exc)
    # primeira execução: cria admin com senha aleatória
    password = secrets.token_urlsafe(9)
    salt = secrets.token_hex(16)
    data = {"username": "admin", "salt": salt, "pw_hash": _hash_pw(password, salt),
            "_plain": password}   # removido do disco; fica só em memória
    AUTH_PATH.write_text(json.dumps({k: v for k, v in data.items() if k != "_plain"},
                                    indent=2))
    print("\n" + "=" * 56)
    print("  ACESSO AO PAINEL CRIADO")
    print("  usuário: admin")
    print(f"  senha:   {password}")
    print("  (troque a senha depois, dentro do painel)")
    print("=" * 56 + "\n")
    return data


AUTH = load_or_create_auth()


def check_credentials(username, password):
    if username != AUTH.get("username"):
        return False
    calc = _hash_pw(password, AUTH["salt"])
    return hmac.compare_digest(calc, AUTH["pw_hash"])


def new_session():
    sid = secrets.token_urlsafe(24)
    _SESSIONS[sid] = time.time() + SESSION_TTL
    return sid


def session_valid(sid):
    if not sid:
        return False
    exp = _SESSIONS.get(sid)
    if not exp:
        return False
    if time.time() > exp:
        _SESSIONS.pop(sid, None)
        return False
    _SESSIONS[sid] = time.time() + SESSION_TTL   # renova a validade
    return True


def is_authed(request):
    return session_valid(request.cookies.get(SESSION_COOKIE))


# ---------------------------------------------------------------------------
# Cliente MQTT (um por impressora, cada um na sua thread)
# ---------------------------------------------------------------------------
def resolve_connection(cfg: dict):
    """Devolve (host, username, password, is_cloud) conforme o modo."""
    mode = cfg.get("mode", "lan")
    if mode == "cloud":
        region = cfg.get("region", "us")
        host = f"{region}.mqtt.bambulab.com"
        uid = str(cfg["uid"])
        username = uid if uid.startswith("u_") else f"u_{uid}"
        print(f"[mqtt] host={host} user={username} token_len={len(cfg.get('token',''))} token_prefix={cfg.get('token','')[:10]}")
        return host, username, cfg["token"], True
    return cfg["ip"], "bblp", cfg["access_code"], False


def start_anycubic_printer(cfg: dict):
    """Monitora uma Kobra em modo LAN via MQTT local (tempo real)."""
    name = cfg["name"]
    ip = cfg.get("ip") or cfg.get("serial") or ""

    with STATE_LOCK:
        STATE[name] = {"_meta": {"name": name, "online": False,
                                 "apelido": cfg.get("apelido", ""),
                                 "brand": "anycubic",
                                 "mode": cfg.get("mode", "lan"),
                                 "ip": cfg.get("ip", ""),
                                 "model": cfg.get("model"),
                                 "has_camera": bool(ip),
                                 "camera_url": cfg.get("camera_url")
                                 or (f"http://{ip}:18088/flv" if ip else None)}}

    stop_flag = threading.Event()
    holder = {"client": None}

    def run():
        import paho.mqtt.client as mqtt
        while not stop_flag.is_set():
            hs = anycubic_lan_handshake(ip)
            if not hs:
                with STATE_LOCK:
                    STATE.setdefault(name, {"_meta": {}})["_meta"]["online"] = False
                broadcaster.notify_from_thread()
                stop_flag.wait(15)  # tenta de novo em 15s
                continue

            device_id = hs["device_id"]
            mode_id = hs["mode_id"]
            base = f"anycubic/anycubicCloud/v1/web/printer/{mode_id}/{device_id}"
            rep1 = f"anycubic/anycubicCloud/v1/+/public/{mode_id}/{device_id}/+/report"
            rep2 = f"anycubic/anycubicCloud/v1/printer/+/{mode_id}/{device_id}/#"

            def on_connect(client, userdata, flags, rc, *a):
                ok = str(rc) in ("0", "Success")
                with STATE_LOCK:
                    m = STATE.setdefault(name, {"_meta": {}})["_meta"]
                    m["online"] = ok
                    m["auth_error"] = False
                    if hs.get("model_name"):
                        m["model"] = hs["model_name"]
                if ok:
                    client.subscribe(rep1)
                    client.subscribe(rep2)
                    _anycubic_query(client, base)
                broadcaster.notify_from_thread()

            def on_message(client, userdata, msg):
                try:
                    payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
                except Exception:
                    return
                data = payload.get("data")
                if not isinstance(data, dict):
                    return
                # Captura a URL dinâmica da câmera (vem no report info)
                urls = data.get("urls")
                if isinstance(urls, dict) and urls.get("rtspUrl"):
                    with STATE_LOCK:
                        STATE.setdefault(name, {"_meta": {}})["_meta"]["cam_stream_url"] = urls["rtspUrl"]
                translated = anycubic_translate_lan(data)
                if translated is None:
                    return
                with STATE_LOCK:
                    st = STATE.setdefault(name, {"_meta": {}})
                    st["_meta"]["online"] = True
                    st.setdefault("print", {}).update(translated)
                    cur_print = dict(st.get("print", {}))
                broadcaster.notify_from_thread()
                try:
                    history_db.observe(name, cur_print, brand="anycubic")
                except Exception:
                    pass

            def on_disconnect(client, userdata, *a):
                with STATE_LOCK:
                    STATE.setdefault(name, {"_meta": {}})["_meta"]["online"] = False
                broadcaster.notify_from_thread()

            try:
                client = mqtt.Client(
                    client_id=f"hub3d-{name}-{int(time.time())}",
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            except Exception:
                client = mqtt.Client(client_id=f"hub3d-{name}-{int(time.time())}")
            holder["client"] = client
            holder["base"] = base
            holder["device_id"] = device_id
            holder["mode_id"] = mode_id
            client.username_pw_set(hs["username"], hs["password"])
            client.tls_set(cert_reqs=ssl.CERT_NONE)
            client.tls_insecure_set(True)
            client.on_connect = on_connect
            client.on_message = on_message
            client.on_disconnect = on_disconnect

            try:
                client.connect(hs["broker_host"], hs["broker_port"], keepalive=30)
            except Exception as exc:
                print(f"[anycubic {name}] erro ao conectar: {exc}")
                stop_flag.wait(15)
                continue

            client.loop_start()
            # pede status periodicamente enquanto conectado
            while not stop_flag.is_set():
                if stop_flag.wait(10):
                    break
                try:
                    _anycubic_query(client, base)
                except Exception:
                    break  # reconecta (refaz handshake, token pode ter mudado)
            client.loop_stop()
            try:
                client.disconnect()
            except Exception:
                pass

    thread = threading.Thread(target=run, daemon=True, name=f"anycubic-{name}")
    PRINTERS[name] = {"client": holder, "stop": stop_flag, "thread": thread}
    thread.start()
    print(f"[anycubic {name}] modo LAN em {ip}")


def _anycubic_query(client, base_topic):
    """Pede o status atual da impressora (info, print, caixa multicolor, vídeo)."""
    for kind in ("info", "print", "multiColorBox"):
        req = {"type": kind, "action": "query",
               "timestamp": int(time.time() * 1000),
               "msgid": str(uuid.uuid4()), "data": None}
        client.publish(f"{base_topic}/{kind}", json.dumps(req))
    # Mantém a câmera ativa (a URL FLV vem no report info)
    vid = {"type": "video", "action": "startCapture",
           "timestamp": int(time.time() * 1000),
           "msgid": str(uuid.uuid4()), "data": None}
    client.publish(f"{base_topic}/video", json.dumps(vid))


def start_printer(cfg: dict):
    # Anycubic usa polling HTTP, não MQTT — desvia para o adapter próprio.
    if cfg.get("brand") == "anycubic":
        return start_anycubic_printer(cfg)

    name = cfg["name"]
    serial = cfg["serial"]
    report_topic = f"device/{serial}/report"
    request_topic = f"device/{serial}/request"
    host, username, password, is_cloud = resolve_connection(cfg)

    with STATE_LOCK:
        STATE[name] = {"_meta": {"name": name, "online": False,
                                 "apelido": cfg.get("apelido", ""),
                                 "brand": cfg.get("brand", "bambu"),
                                 "mode": cfg.get("mode", "cloud"),
                                 "ip": cfg.get("ip", ""),
                                 "model": cfg.get("model"),
                                 "has_camera": bool(cfg.get("ip") and cfg.get("access_code")),
                                 "camera_url": cfg.get("camera_url")}}

    def on_connect(client, userdata, flags, reason_code, properties=None):
        ok = (reason_code == 0)
        print(f"[{name}] conectado: {reason_code}")
        with STATE_LOCK:
            meta = STATE[name].setdefault("_meta", {})
            meta["online"] = ok
            # "Not authorized" (code 5 / 135) = token expirado ou inválido
            code_str = str(reason_code).lower()
            if "not authorized" in code_str or "unauthorized" in code_str:
                meta["auth_error"] = True
            elif ok:
                meta["auth_error"] = False
        if ok:
            client.subscribe(report_topic)
            # pushall: pede o estado completo (essencial na série P1, que
            # normalmente só manda deltas).
            client.publish(
                request_topic,
                json.dumps({"pushing": {"sequence_id": "0", "command": "pushall"}}),
            )
        broadcaster.notify_from_thread()

    def on_disconnect(client, userdata, *args):
        print(f"[{name}] desconectado")
        with STATE_LOCK:
            STATE[name].setdefault("_meta", {})["online"] = False
        broadcaster.notify_from_thread()

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        except Exception:
            return
        with STATE_LOCK:
            deep_merge(STATE[name], data)
            STATE[name].setdefault("_meta", {})["online"] = True
            current_print = dict(STATE[name].get("print", {}))
        broadcaster.notify_from_thread()
        try:
            supabase_logger.observe(name, current_print)
            history_db.observe(name, current_print, brand=cfg.get("brand", "bambu"))
        except Exception:
            pass

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"dashboard-{serial}",
    )
    client.username_pw_set(username, password)
    if is_cloud:
        # A nuvem da Bambu tem certificado público válido: validação normal.
        client.tls_set()
    else:
        # O broker da impressora usa certificado autoassinado: aceitamos sem
        # validar o hostname/cadeia.
        client.tls_set(cert_reqs=ssl.CERT_NONE)
        client.tls_insecure_set(True)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=2, max_delay=30)

    stop_flag = threading.Event()

    def run():
        while not stop_flag.is_set():
            try:
                client.connect(host, 8883, keepalive=30)
                client.loop_forever(retry_first_connection=True)
            except Exception as exc:
                if stop_flag.is_set():
                    break
                print(f"[{name}] erro de conexão: {exc} — tentando de novo em 5s")
                time.sleep(5)

    thread = threading.Thread(target=run, daemon=True, name=f"mqtt-{name}")
    PRINTERS[name] = {"client": client, "stop": stop_flag, "thread": thread}
    thread.start()


# ---------------------------------------------------------------------------
# Servidor web
# ---------------------------------------------------------------------------
app = FastAPI()


@app.on_event("startup")
async def _startup():
    broadcaster.loop = asyncio.get_event_loop()


ATIVACAO_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ativação · 3DWORK</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:'Segoe UI',system-ui,sans-serif; background:#0a0e16;
    color:#e8edf5; min-height:100vh; display:flex; align-items:center;
    justify-content:center; padding:1.5rem}
  .box{background:#111725; border:1px solid #1e2838; border-radius:18px;
    padding:2.2rem; max-width:520px; width:100%; box-shadow:0 20px 60px -20px #000}
  .logo{height:44px; margin-bottom:1.4rem}
  h1{font-size:1.35rem; margin-bottom:.4rem}
  .sub{color:#8a96a8; font-size:.9rem; line-height:1.5; margin-bottom:1.6rem}
  .fp-label{font-size:.72rem; text-transform:uppercase; letter-spacing:.1em;
    color:#8a96a8; margin-bottom:.5rem}
  .fp-box{display:flex; gap:.5rem; align-items:center; margin-bottom:1.6rem}
  .fp{flex:1; font-family:'Consolas',monospace; font-size:1.15rem; font-weight:700;
    letter-spacing:.05em; background:#0a0e16; border:1px solid #263042;
    border-radius:10px; padding:.8rem 1rem; color:#4fd1ff; text-align:center}
  .copy{background:#1a2333; border:1px solid #263042; border-radius:10px;
    padding:.8rem 1rem; cursor:pointer; color:#8a96a8; font-size:.85rem; white-space:nowrap}
  .copy:hover{color:#fff; border-color:#4fd1ff}
  .steps{background:#0d1420; border:1px solid #1e2838; border-radius:12px;
    padding:1rem 1.2rem; margin-bottom:1.6rem; font-size:.85rem; line-height:1.7; color:#b8c2d0}
  .steps b{color:#fff}
  label{display:block; font-size:.8rem; color:#8a96a8; margin-bottom:.5rem}
  textarea{width:100%; min-height:90px; background:#0a0e16; border:1px solid #263042;
    border-radius:10px; padding:.8rem; color:#e8edf5; font-family:'Consolas',monospace;
    font-size:.78rem; resize:vertical}
  textarea:focus{outline:none; border-color:#4fd1ff}
  .btn{width:100%; margin-top:1rem; background:#00AFF0; color:#04122e; border:0;
    border-radius:10px; padding:.9rem; font-size:.95rem; font-weight:700; cursor:pointer}
  .btn:hover{background:#0098d4}
  .wpp-btn{display:block; margin-top:.7rem; text-align:center; text-decoration:none;
    background:#1a2333; border:1px solid #25D366; color:#25D366; border-radius:10px;
    padding:.75rem; font-size:.88rem; font-weight:600}
  .wpp-btn:hover{background:#25D366; color:#04122e}
  .msg{margin-top:1rem; padding:.8rem 1rem; border-radius:10px; font-size:.85rem; display:none}
  .msg.err{background:#3a1a20; border:1px solid #6a2a35; color:#ff8095; display:block}
  .msg.ok{background:#1a3a25; border:1px solid #2a6a45; color:#80ffa5; display:block}
</style></head><body>
  <div class="box">
    <img class="logo" src="__LOGO_SRC__" alt="3DWORK">
    <h1>Ativação necessária</h1>
    <p class="sub">Este sistema precisa de uma licença válida para funcionar.
      Envie o <b>Código da Máquina</b> abaixo para o fornecedor e cole a
      chave de licença que você receber.</p>

    <div class="fp-label">Código desta máquina</div>
    <div class="fp-box">
      <div class="fp" id="fp">__FINGERPRINT__</div>
      <button class="copy" onclick="copyFp()">Copiar</button>
    </div>

    <div class="steps">
      <b>1.</b> Copie o Código da Máquina acima<br>
      <b>2.</b> Envie ao fornecedor (WhatsApp / e-mail)<br>
      <b>3.</b> Cole abaixo a chave de licença recebida<br>
      <b>4.</b> Clique em Ativar
    </div>

    <label>Chave de licença</label>
    <textarea id="chave" placeholder="Cole aqui a chave de licença..."></textarea>
    <button class="btn" onclick="ativar()">Ativar sistema</button>
    <a class="wpp-btn" id="wppBtn" href="#" target="_blank">💬 Falar no WhatsApp / Comprar licença</a>
    <div class="msg" id="msg"></div>
  </div>
<script>
// Monta o link do WhatsApp já com o Código da Máquina
(function(){
  const fp=document.getElementById("fp").textContent.trim();
  const msg=`Olá! Gostaria de comprar a licença do 3DWORK FARM.\n\nCódigo da Máquina: ${fp}`;
  const btn=document.getElementById("wppBtn");
  if(btn) btn.href=`https://wa.me/5512988447240?text=${encodeURIComponent(msg)}`;
})();
function copyFp(){
  const fp=document.getElementById("fp").textContent.trim();
  navigator.clipboard.writeText(fp).then(()=>{
    const b=event.target; const t=b.textContent; b.textContent="Copiado!";
    setTimeout(()=>b.textContent=t,1500);
  });
}
async function ativar(){
  const chave=document.getElementById("chave").value.trim();
  const msg=document.getElementById("msg");
  if(!chave){ msg.className="msg err"; msg.textContent="Cole a chave de licença primeiro."; return; }
  try{
    const r=await fetch("/api/ativar",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({chave})});
    const d=await r.json();
    if(d.ok){
      msg.className="msg ok"; msg.textContent=d.msg+" Redirecionando...";
      setTimeout(()=>location.href="/",1200);
    }else{
      msg.className="msg err"; msg.textContent=d.msg;
    }
  }catch(_){ msg.className="msg err"; msg.textContent="Erro ao ativar. Tente de novo."; }
}
</script>
</body></html>"""


LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Entrar · 3DWORK Farm de Impressoras</title>
<link rel="icon" href="__LOGO_SRC__">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
  :root{--void:#070a10;--panel:#0d121b;--hair:#1c2535;--hair-lit:#2a3a54;
    --ink:#e9eef7;--muted:#8593a8;--faint:#56627b;--live:#4f8cff;--fail:#ff5470}
  *{box-sizing:border-box}
  body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;
    font-family:'Space Grotesk',system-ui,sans-serif;color:var(--ink);
    background:radial-gradient(800px 460px at 50% -10%,#142035,transparent 62%),var(--void)}
  .box{width:min(380px,92vw);background:linear-gradient(180deg,var(--panel),#0b0f17);
    border:1px solid var(--hair);border-radius:18px;padding:2rem 1.8rem;
    box-shadow:0 30px 80px -30px #000;position:relative}
  .box::before{content:"";position:absolute;top:0;left:0;right:0;height:1px;
    background:linear-gradient(90deg,transparent,var(--hair-lit),transparent);opacity:.7}
  .logo{display:block;margin:0 auto 1.2rem;height:44px;
    filter:drop-shadow(0 1px 7px rgba(79,140,255,.3))}
  h1{font-size:.7rem;letter-spacing:.2em;text-transform:uppercase;color:var(--faint);
    text-align:center;font-weight:500;margin:0 0 1.6rem}
  label{display:block;font-family:'JetBrains Mono',monospace;font-size:.6rem;
    letter-spacing:.12em;text-transform:uppercase;color:var(--faint);margin:.9rem 0 .35rem}
  input{width:100%;background:#0a0e16;border:1px solid var(--hair);border-radius:9px;
    padding:.7rem .8rem;color:var(--ink);font-family:'JetBrains Mono',monospace;font-size:.9rem}
  input:focus{outline:none;border-color:var(--live)}
  button{width:100%;margin-top:1.5rem;padding:.75rem;border:none;border-radius:9px;
    background:var(--live);color:#04122e;font-weight:700;font-size:.92rem;cursor:pointer;
    font-family:'Space Grotesk',sans-serif;letter-spacing:.02em}
  button:hover{filter:brightness(1.08)}
  .err{color:var(--fail);font-family:'JetBrains Mono',monospace;font-size:.74rem;
    text-align:center;margin-top:1rem;min-height:1em}
</style>
</head>
<body>
  <form class="box" onsubmit="return doLogin(event)">
    <img class="logo" src="__LOGO_SRC__" alt="3D WORK">
    <h1>3DWORK · Farm de Impressoras</h1>
    <label for="u">Usuário</label>
    <input id="u" autocomplete="username" autofocus>
    <label for="p">Senha</label>
    <input id="p" type="password" autocomplete="current-password">
    <button type="submit">Entrar</button>
    <div class="err" id="err"></div>
  </form>
<script>
async function doLogin(e){
  e.preventDefault();
  const err=document.getElementById("err"); err.textContent="";
  try{
    const r=await fetch("/login",{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({username:document.getElementById("u").value,
                           password:document.getElementById("p").value})});
    if(r.ok){ location.href="/"; }
    else { const d=await r.json().catch(()=>({})); err.textContent=d.error||"Falha no login."; }
  }catch(_){ err.textContent="Erro de conexão."; }
  return false;
}
</script>
</body>
</html>"""


@app.get("/login")
async def login_page(request: Request):
    if is_authed(request):
        return RedirectResponse("/", status_code=302)
    return HTMLResponse(LOGIN_HTML.replace("__LOGO_SRC__", get_logo_uri()))


@app.post("/login")
async def login_submit(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if check_credentials(body.get("username", ""), body.get("password", "")):
        resp = JSONResponse({"ok": True})
        resp.set_cookie(SESSION_COOKIE, new_session(), max_age=SESSION_TTL,
                        httponly=True, samesite="lax")
        return resp
    return JSONResponse({"ok": False, "error": "Usuário ou senha inválidos."}, status_code=401)


@app.get("/logout")
async def logout(request: Request):
    _SESSIONS.pop(request.cookies.get(SESSION_COOKIE), None)
    resp = RedirectResponse("/login", status_code=302)
    resp.delete_cookie(SESSION_COOKIE)
    return resp


@app.post("/account/password")
async def change_password(request: Request):
    if not is_authed(request):
        return JSONResponse({"ok": False, "error": "Não autenticado."}, status_code=401)
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not check_credentials(AUTH.get("username"), body.get("current", "")):
        return JSONResponse({"ok": False, "error": "Senha atual incorreta."}, status_code=400)
    new_pw = body.get("new", "")
    if len(new_pw) < 6:
        return JSONResponse({"ok": False, "error": "A nova senha precisa ter ao menos 6 caracteres."}, status_code=400)
    salt = secrets.token_hex(16)
    AUTH["salt"] = salt
    AUTH["pw_hash"] = _hash_pw(new_pw, salt)
    AUTH_PATH.write_text(json.dumps(AUTH, indent=2))
    return JSONResponse({"ok": True})


@app.get("/")
async def index(request: Request):
    # Gate de licença: sem licença válida, vai para a tela de ativação
    if not LICENSE_STATE.get("ok"):
        return RedirectResponse("/ativar", status_code=302)
    if not is_authed(request):
        return RedirectResponse("/login", status_code=302)
    html = DASHBOARD_HTML.replace("__LOGO_SRC__", get_logo_uri()) \
                         .replace("__APP_VERSION__", APP_VERSION)
    return HTMLResponse(html)


@app.get("/ativar")
async def ativar_page(request: Request):
    if LICENSE_STATE.get("ok"):
        return RedirectResponse("/", status_code=302)
    fp = LICENSE_STATE.get("fingerprint") or machine_fingerprint()
    html = ATIVACAO_HTML.replace("__FINGERPRINT__", fp).replace("__LOGO_SRC__", get_logo_uri())
    return HTMLResponse(html)


@app.post("/api/ativar")
async def api_ativar(request: Request):
    body = await request.json()
    key = body.get("chave", "")
    ok, msg = await asyncio.to_thread(install_license, key)
    if ok:
        refresh_license()
    return {"ok": ok, "msg": msg}


@app.post("/api/logo")
async def api_upload_logo(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    data_uri = body.get("data", "")
    # Espera algo como "data:image/png;base64,...."
    if not data_uri.startswith("data:image/"):
        return {"ok": False, "msg": "Formato inválido. Envie uma imagem PNG ou JPG."}
    try:
        header, b64 = data_uri.split(",", 1)
        is_jpg = "jpeg" in header or "jpg" in header
        raw = base64.b64decode(b64)
        if len(raw) > 3 * 1024 * 1024:
            return {"ok": False, "msg": "Imagem muito grande (máx. 3 MB)."}
        # remove logos antigos e salva o novo
        for nome in _LOGO_CUSTOM_NAMES:
            Path(__file__).with_name(nome).unlink(missing_ok=True)
        ext = "jpg" if is_jpg else "png"
        Path(__file__).with_name(f"logo_cliente.{ext}").write_bytes(raw)
        return {"ok": True, "msg": "Logo atualizado com sucesso!"}
    except Exception:
        return {"ok": False, "msg": "Não foi possível salvar a imagem."}


@app.post("/api/logo/reset")
async def api_reset_logo(request: Request):
    if (block := _need_auth(request)):
        return block
    for nome in _LOGO_CUSTOM_NAMES:
        Path(__file__).with_name(nome).unlink(missing_ok=True)
    return {"ok": True, "msg": "Logo padrão restaurado."}


@app.get("/api/licenca/info")
async def api_licenca_info(request: Request):
    if (block := _need_auth(request)):
        return block
    st = LICENSE_STATE or {}
    return {"ok": st.get("ok", False),
            "fingerprint": st.get("fingerprint", ""),
            "cliente": st.get("cliente", "")}


# --- Atualização do sistema (semiautomática via GitHub) ---------------------
def _parse_version(v):
    """'1.2.3' -> (1,2,3) para comparar de forma numérica."""
    try:
        return tuple(int(x) for x in str(v).strip().split("."))
    except Exception:
        return (0,)


def _version_maior(nova, atual):
    a, b = _parse_version(nova), _parse_version(atual)
    # normaliza o tamanho das tuplas
    n = max(len(a), len(b))
    a = a + (0,) * (n - len(a))
    b = b + (0,) * (n - len(b))
    return a > b


def _check_update():
    """Consulta o GitHub e diz se há versão mais nova. Não baixa nada."""
    if not requests or "SEU_USUARIO" in UPDATE_INFO_URL:
        return {"ok": True, "disponivel": False, "atual": APP_VERSION,
                "motivo": "nao_configurado"}
    try:
        r = requests.get(UPDATE_INFO_URL, timeout=6,
                         headers={"Cache-Control": "no-cache"})
        if r.status_code != 200:
            return {"ok": False, "erro": "Não foi possível consultar atualizações."}
        info = r.json()
        nova = str(info.get("versao", "")).strip()
        return {
            "ok": True,
            "atual": APP_VERSION,
            "nova": nova,
            "disponivel": bool(nova) and _version_maior(nova, APP_VERSION),
            "notas": info.get("notas", ""),
            "url_arquivo": info.get("url_arquivo", ""),
            "obrigatoria": bool(info.get("obrigatoria", False)),
        }
    except Exception as exc:
        return {"ok": False, "erro": f"Falha ao verificar: {exc}"}


@app.get("/api/update/check")
async def api_update_check(request: Request):
    if (block := _need_auth(request)):
        return block
    return await asyncio.to_thread(_check_update)


@app.post("/api/update/apply")
async def api_update_apply(request: Request):
    """Baixa a nova versão, faz backup do arquivo atual e substitui."""
    if (block := _need_auth(request)):
        return block
    info = await asyncio.to_thread(_check_update)
    if not info.get("ok") or not info.get("disponivel"):
        return {"ok": False, "erro": "Nenhuma atualização disponível."}
    url = info.get("url_arquivo", "")
    if not url or not requests:
        return {"ok": False, "erro": "Link da nova versão não informado."}

    def _baixar_e_aplicar():
        try:
            r = requests.get(url, timeout=30, headers={"Cache-Control": "no-cache"})
            if r.status_code != 200 or len(r.content) < 1000:
                return {"ok": False, "erro": "Download da nova versão falhou."}
            novo = r.content
            # sanidade: precisa parecer o nosso arquivo Python
            if b"DASHBOARD_HTML" not in novo or b"APP_VERSION" not in novo:
                return {"ok": False, "erro": "O arquivo baixado não parece válido."}
            arquivo = Path(__file__)
            # backup com timestamp
            backup = arquivo.with_name(
                f"bambu_dashboard.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py")
            try:
                backup.write_bytes(arquivo.read_bytes())
            except Exception:
                pass
            arquivo.write_bytes(novo)
            return {"ok": True, "nova": info.get("nova", ""),
                    "backup": backup.name}
        except Exception as exc:
            return {"ok": False, "erro": f"Erro ao atualizar: {exc}"}

    return await asyncio.to_thread(_baixar_e_aplicar)


# --- Calculadora de custo: configurações salvas -----------------------------
CALC_PATH = Path(__file__).with_name("calculadora.json")

CALC_DEFAULT = {
    "preco_kg": 120.0,        # R$ por kg de filamento
    "potencia_w": 150.0,      # consumo médio da impressora em watts
    "preco_kwh": 0.95,        # R$ por kWh
    "valor_maquina": 3000.0,  # valor da impressora
    "vida_util_h": 5000.0,    # horas de vida útil estimada
    "margem_pct": 100.0,      # margem de lucro em %
    "falha_pct": 5.0,         # % de perda por falhas
}


def load_calc_cfg():
    try:
        if CALC_PATH.exists():
            data = json.loads(CALC_PATH.read_text(encoding="utf-8"))
            cfg = dict(CALC_DEFAULT)
            cfg.update({k: v for k, v in data.items() if k in CALC_DEFAULT})
            return cfg
    except Exception:
        pass
    return dict(CALC_DEFAULT)


@app.post("/api/custo/set")
async def api_custo_set(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    name = body.get("name", "")
    if not name:
        return {"ok": False, "error": "Impressora não informada."}
    cfg = load_calc_cfg()
    try:
        peso_g = float(body.get("peso_g") or 0)
        preco_kg = float(body.get("preco_kg") or cfg["preco_kg"])
        minutos = float(body.get("minutos") or 0)
        preco_kwh = float(body.get("preco_kwh") or cfg["preco_kwh"])
    except (TypeError, ValueError):
        return {"ok": False, "error": "Valores inválidos."}
    if peso_g <= 0:
        return {"ok": False, "error": "Informe o peso da peça."}

    custo, c_mat, c_ener = calcular_custo(peso_g, preco_kg, minutos,
                                          preco_kwh, cfg["potencia_w"])
    set_print_cost(name, {
        "file": body.get("file", ""),
        "material": body.get("material", "PLA"),
        "peso_g": peso_g, "preco_kg": preco_kg,
        "minutos": minutos, "preco_kwh": preco_kwh,
        "custo": custo, "custo_material": c_mat, "custo_energia": c_ener,
        "skip": False,
    })
    return {"ok": True, "custo": custo, "material": c_mat, "energia": c_ener}


@app.post("/api/custo/skip")
async def api_custo_skip(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    name = body.get("name", "")
    if not name:
        return {"ok": False, "error": "Impressora não informada."}
    set_print_cost(name, {"file": body.get("file", ""), "skip": True})
    return {"ok": True}


# --- Gerenciador de projetos ------------------------------------------------
@app.get("/api/projetos/list")
async def api_proj_list(request: Request):
    if (block := _need_auth(request)):
        return block
    local = request.query_params.get("local", "local")
    rel = request.query_params.get("path", "")
    return await asyncio.to_thread(proj_list, local, rel)


@app.get("/api/projetos/config")
async def api_proj_config(request: Request):
    if (block := _need_auth(request)):
        return block
    cfg = _proj_cfg()
    cd = cfg.get("cloud_dir", "")
    return {"ok": True, "cloud_dir": cd, "cloud_ok": bool(cd and Path(cd).exists())}


@app.post("/api/projetos/config")
async def api_proj_config_save(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    cd = (body.get("cloud_dir") or "").strip()
    if cd and not Path(cd).exists():
        return {"ok": False, "error": "Essa pasta não existe no computador."}
    _proj_save_cfg({"cloud_dir": cd})
    return {"ok": True, "cloud_dir": cd}


@app.post("/api/projetos/mkdir")
async def api_proj_mkdir(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    local = body.get("local", "local")
    rel = body.get("path", "")
    nome = (body.get("nome") or "").strip()
    if not nome or "/" in nome or "\\" in nome or nome.startswith("."):
        return {"ok": False, "error": "Nome de pasta inválido."}
    root = _proj_root(local)
    if root is None:
        return {"ok": False, "error": "cloud_nao_configurada"}
    target = _proj_safe(root, (rel + "/" + nome) if rel else nome)
    if target is None:
        return {"ok": False, "error": "Caminho inválido."}
    try:
        target.mkdir(parents=True, exist_ok=True)
    except Exception:
        return {"ok": False, "error": "Não foi possível criar a pasta."}
    return {"ok": True}


@app.post("/api/projetos/upload")
async def api_proj_upload(request: Request, file: UploadFile = File(...),
                          local: str = Form("local"), path: str = Form("")):
    if (block := _need_auth(request)):
        return block
    ext = Path(file.filename).suffix.lower()
    if ext not in PROJ_EXTS:
        return {"ok": False, "error": f"Tipo não aceito ({ext}). Use STL, 3MF, OBJ ou G-code."}
    root = _proj_root(local)
    if root is None:
        return {"ok": False, "error": "cloud_nao_configurada"}
    safe_name = Path(file.filename).name
    target = _proj_safe(root, (path + "/" + safe_name) if path else safe_name)
    if target is None:
        return {"ok": False, "error": "Caminho inválido."}
    target.parent.mkdir(parents=True, exist_ok=True)
    size = 0
    limit = PROJ_MAX_MB * 1024 * 1024
    try:
        with open(target, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > limit:
                    out.close()
                    target.unlink(missing_ok=True)
                    return {"ok": False, "error": f"Arquivo maior que {PROJ_MAX_MB} MB."}
                out.write(chunk)
    except Exception:
        return {"ok": False, "error": "Falha ao salvar o arquivo."}
    return {"ok": True, "name": safe_name}


@app.get("/api/projetos/download")
async def api_proj_download(request: Request):
    if (block := _need_auth(request)):
        return block
    local = request.query_params.get("local", "local")
    rel = request.query_params.get("path", "")
    root = _proj_root(local)
    target = _proj_safe(root, rel) if root else None
    if target is None or not target.is_file():
        return JSONResponse({"ok": False, "error": "Arquivo não encontrado."}, status_code=404)
    return FileResponse(str(target), filename=target.name)


@app.post("/api/projetos/rename")
async def api_proj_rename(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    local = body.get("local", "local")
    rel = body.get("path", "")
    novo = (body.get("nome") or "").strip()
    if not novo or "/" in novo or "\\" in novo:
        return {"ok": False, "error": "Nome inválido."}
    root = _proj_root(local)
    src = _proj_safe(root, rel) if root else None
    if src is None or not src.exists():
        return {"ok": False, "error": "Item não encontrado."}
    # preserva a extensão em arquivos
    if src.is_file() and "." in src.name and not novo.endswith(src.suffix):
        novo = novo + src.suffix
    dst = src.parent / novo
    try:
        src.rename(dst)
    except Exception:
        return {"ok": False, "error": "Não foi possível renomear."}
    return {"ok": True}


@app.post("/api/projetos/mover")
async def api_proj_mover(request: Request):
    """Move um arquivo/pasta para dentro de outra pasta (destino)."""
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    local = body.get("local", "local")
    origem = body.get("origem", "")           # caminho relativo do item
    destino = body.get("destino", "")         # pasta destino ("" = raiz)
    root = _proj_root(local)
    if root is None:
        return {"ok": False, "error": "cloud_nao_configurada"}
    src = _proj_safe(root, origem)
    if src is None or not src.exists() or src == root:
        return {"ok": False, "error": "Item não encontrado."}
    dst_dir = _proj_safe(root, destino)
    if dst_dir is None or not dst_dir.exists() or not dst_dir.is_dir():
        return {"ok": False, "error": "Pasta de destino inválida."}
    # não deixar mover para a pasta onde já está
    if src.parent.resolve() == dst_dir.resolve():
        return {"ok": True}                   # nada a fazer
    # não deixar mover uma pasta para dentro dela mesma
    try:
        dst_dir.resolve().relative_to(src.resolve())
        return {"ok": False, "error": "Não é possível mover uma pasta para dentro dela mesma."}
    except ValueError:
        pass
    alvo = dst_dir / src.name
    if alvo.exists():
        return {"ok": False, "error": f'Já existe "{src.name}" na pasta de destino.'}
    try:
        _shutil.move(str(src), str(alvo))
    except Exception:
        return {"ok": False, "error": "Não foi possível mover."}
    return {"ok": True}


@app.post("/api/projetos/delete")
async def api_proj_delete(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    local = body.get("local", "local")
    rel = body.get("path", "")
    root = _proj_root(local)
    target = _proj_safe(root, rel) if root else None
    if target is None or not target.exists() or target == root:
        return {"ok": False, "error": "Item não encontrado."}
    try:
        if target.is_dir():
            _shutil.rmtree(target)
        else:
            target.unlink()
    except Exception:
        return {"ok": False, "error": "Não foi possível excluir."}
    return {"ok": True}


@app.get("/api/projetos/fatiadores")
async def api_proj_fatiadores(request: Request):
    """Lista os programas fatiadores para o usuário escolher."""
    if (block := _need_auth(request)):
        return block
    out = []
    for brand, info in SLICER_DEFAULTS.items():
        out.append({
            "brand": brand,
            "nome": info.get("nome", brand),
            "instalado": _find_slicer(brand) is not None,
        })
    return {"ok": True, "fatiadores": out}


@app.post("/api/projetos/abrir")
async def api_proj_abrir(request: Request):
    """Abre um arquivo no fatiador escolhido."""
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    local = body.get("local", "local")
    rel = body.get("path", "")
    brand = body.get("brand", "")
    if brand not in SLICER_DEFAULTS:
        return {"ok": False, "error": "Fatiador inválido."}
    root = _proj_root(local)
    target = _proj_safe(root, rel) if root else None
    if target is None or not target.is_file():
        return {"ok": False, "error": "Arquivo não encontrado."}
    ok, err = _open_in_slicer(brand, target)
    if not ok:
        return {"ok": False, "error": err, "brand": brand,
                "slicer": SLICER_DEFAULTS.get(brand, {}).get("nome", "fatiador"),
                "precisa_caminho": True}
    return {"ok": True}


@app.post("/api/projetos/slicer_path")
async def api_proj_slicer_path(request: Request):
    """Salva o caminho do executável do fatiador de uma marca."""
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    brand = body.get("brand", "")
    caminho = (body.get("path") or "").strip().strip('"')
    if brand not in SLICER_DEFAULTS:
        return {"ok": False, "error": "Fatiador inválido."}
    if not caminho or not Path(caminho).exists():
        return {"ok": False, "error": "Esse arquivo não existe no computador."}
    cfg = _slicer_cfg()
    cfg[brand] = caminho
    _slicer_save_cfg(cfg)
    return {"ok": True}


@app.get("/api/calc/config")
async def api_calc_get(request: Request):
    if (block := _need_auth(request)):
        return block
    return {"ok": True, "cfg": load_calc_cfg()}


@app.post("/api/calc/config")
async def api_calc_save(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    cfg = load_calc_cfg()
    for k in CALC_DEFAULT:
        if k in body:
            try:
                cfg[k] = float(body[k])
            except (TypeError, ValueError):
                pass
    try:
        CALC_PATH.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    except Exception:
        return {"ok": False, "error": "Não foi possível salvar."}
    return {"ok": True, "cfg": cfg}


# ---------------------------------------------------------------------------
# Câmera — relay de vídeo para o navegador (via ffmpeg)
#   Kobra:     FLV em http://IP:18088/live/<token> (URL vem no report MQTT).
#   Bambu A1:  protocolo chamber_image (TCP+TLS na porta 6000).
#   ffmpeg converte ambos em MJPEG, que o navegador exibe num <img>.
# ---------------------------------------------------------------------------
_FFMPEG_PATH = None


def _find_ffmpeg():
    """Localiza o ffmpeg: PATH, pasta local, ou baixa se necessário (Windows)."""
    global _FFMPEG_PATH
    if _FFMPEG_PATH:
        return _FFMPEG_PATH
    import shutil
    # 1. No PATH?
    exe = shutil.which("ffmpeg")
    if exe:
        _FFMPEG_PATH = exe
        return exe
    # 2. Na pasta do sistema?
    here = Path(__file__).parent
    for cand in [here / "ffmpeg" / "ffmpeg.exe", here / "ffmpeg.exe",
                 here / "ffmpeg" / "bin" / "ffmpeg.exe"]:
        if cand.exists():
            _FFMPEG_PATH = str(cand)
            return _FFMPEG_PATH
    return None


def _download_ffmpeg():
    """Baixa uma build estática do ffmpeg para Windows (uma vez)."""
    if not requests:
        return None
    here = Path(__file__).parent
    dest_dir = here / "ffmpeg"
    dest = dest_dir / "ffmpeg.exe"
    if dest.exists():
        return str(dest)
    url = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
    try:
        import zipfile, io
        print("[camera] baixando ffmpeg (uma única vez)...")
        r = requests.get(url, timeout=120, stream=True)
        buf = io.BytesIO(r.content)
        with zipfile.ZipFile(buf) as z:
            for member in z.namelist():
                if member.endswith("bin/ffmpeg.exe"):
                    dest_dir.mkdir(exist_ok=True)
                    with z.open(member) as src, open(dest, "wb") as out:
                        out.write(src.read())
                    print(f"[camera] ffmpeg instalado em {dest}")
                    return str(dest)
    except Exception as exc:
        print(f"[camera] falha ao baixar ffmpeg: {exc}")
    return None


def _ffmpeg_snapshot(url):
    """Extrai um único quadro JPEG de um stream FLV via ffmpeg (otimizado p/ velocidade)."""
    import subprocess
    ffmpeg = _find_ffmpeg() or _download_ffmpeg()
    if not ffmpeg:
        return None
    # analyzeduration/probesize baixos = ffmpeg decide rápido, sem esperar analisar muito
    cmd = [ffmpeg, "-loglevel", "error", "-y",
           "-analyzeduration", "200000", "-probesize", "200000",
           "-fflags", "nobuffer", "-flags", "low_delay",
           "-i", url, "-frames:v", "1", "-q:v", "6",
           "-f", "image2", "-update", "1", "pipe:1"]
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        out, _ = proc.communicate(timeout=8)
        if out[:2] == b"\xff\xd8":  # JPEG válido
            return out
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    return None


def _anycubic_camera_stream(name):
    """
    Câmera da Kobra: sequência de snapshots como MJPEG.
    A Kobra não faz vídeo contínuo, então pegamos ~1 foto/seg.
    """
    print(f"[camera {name}] iniciando stream de snapshots")
    count = 0
    fails = 0
    while True:
        with STATE_LOCK:
            meta = STATE.get(name, {}).get("_meta", {})
            url = meta.get("cam_stream_url")
        if not url:
            print(f"[camera {name}] sem URL de stream — encerrando")
            break
        frame = _ffmpeg_snapshot(url)
        if frame:
            count += 1
            fails = 0
            if count <= 2 or count % 20 == 0:
                print(f"[camera {name}] quadro {count} ok ({len(frame)} bytes)")
            yield (b"\r\n--frame\r\n"
                   b"Content-Type: image/jpeg\r\n"
                   b"Content-Length: " + str(len(frame)).encode() +
                   b"\r\n\r\n" + frame + b"\r\n")
        else:
            fails += 1
            print(f"[camera {name}] falha ao capturar quadro (tentativa {fails})")
            if fails >= 5:
                print(f"[camera {name}] muitas falhas — encerrando")
                break
            time.sleep(0.5)


def _ffmpeg_mjpeg(input_url_or_args, is_bambu=False, bambu_ip=None, bambu_code=None):
    """
    Roda o ffmpeg convertendo a fonte em MJPEG e devolve os quadros.
    Para a Kobra: input_url_or_args é a URL FLV.
    Para a Bambu: lê da porta 6000 via um pipe interno.
    """
    import subprocess
    ffmpeg = _find_ffmpeg() or _download_ffmpeg()
    if not ffmpeg:
        return

    if is_bambu:
        # Bambu: ffmpeg não fala chamber_image; alimentamos JPEGs via stdin
        proc = subprocess.Popen(
            [ffmpeg, "-f", "image2pipe", "-i", "pipe:0",
             "-f", "mpjpeg", "-q:v", "5", "-r", "10", "pipe:1"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        feeder = threading.Thread(target=_feed_bambu_jpegs,
                                  args=(proc, bambu_ip, bambu_code), daemon=True)
        feeder.start()
    else:
        # Kobra: ffmpeg lê o FLV direto da URL
        proc = subprocess.Popen(
            [ffmpeg, "-fflags", "nobuffer", "-flags", "low_delay",
             "-i", input_url_or_args,
             "-f", "mpjpeg", "-q:v", "5", "-r", "10", "pipe:1"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    try:
        while True:
            chunk = proc.stdout.read(8192)
            if not chunk:
                break
            yield chunk
    finally:
        try:
            proc.kill()
        except Exception:
            pass


def _feed_bambu_jpegs(proc, ip, access_code):
    """Lê JPEGs da câmera Bambu (porta 6000) e escreve no stdin do ffmpeg."""
    try:
        for part in _bambu_camera_frames(ip, access_code, raw=True):
            proc.stdin.write(part)
    except Exception:
        pass
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass


def _bambu_camera_frames(ip, access_code, raw=False):
    """
    Gera quadros JPEG da câmera da Bambu (A1/P1) via porta 6000.
    Protocolo: pacote de auth de 80 bytes, depois header de 16 bytes + JPEG.
    raw=True devolve só os bytes do JPEG (para alimentar o ffmpeg).
    raw=False devolve no formato multipart MJPEG (uso direto, sem ffmpeg).
    """
    import socket as _socket
    auth = bytearray()
    auth += (0x40).to_bytes(4, "little")
    auth += (0x3000).to_bytes(4, "little")
    auth += (0).to_bytes(4, "little")
    auth += (0).to_bytes(4, "little")
    for s in (access_code, "bblp"):
        b = s.encode("ascii")[:32]
        auth += b + b"\0" * (32 - len(b))

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    raw_sock = _socket.create_connection((ip, 6000), timeout=8)
    sock = ctx.wrap_socket(raw_sock, server_hostname=ip)
    sock.settimeout(10)
    sock.sendall(bytes(auth))

    JPEG_START = bytes([0xff, 0xd8, 0xff, 0xe0])
    JPEG_END = bytes([0xff, 0xd9])
    buf = bytearray()
    try:
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
            while True:
                start = buf.find(JPEG_START)
                if start < 0:
                    if len(buf) > 4:
                        del buf[:-4]
                    break
                end = buf.find(JPEG_END, start + 4)
                if end < 0:
                    if start > 0:
                        del buf[:start]
                    break
                frame = bytes(buf[start:end + 2])
                del buf[:end + 2]
                if raw:
                    yield frame
                else:
                    yield (b"--frame\r\nContent-Type: image/jpeg\r\n"
                           b"Content-Length: " + str(len(frame)).encode() +
                           b"\r\n\r\n" + frame + b"\r\n")
    finally:
        try:
            sock.close()
        except Exception:
            pass


@app.get("/api/camdebug")
async def cam_debug_all(request: Request):
    if (block := _need_auth(request)):
        return block
    out = {}
    with STATE_LOCK:
        for nm, st in STATE.items():
            meta = st.get("_meta", {})
            out[nm] = {"brand": meta.get("brand"),
                       "online": meta.get("online"),
                       "tem_url": bool(meta.get("cam_stream_url")),
                       "url": meta.get("cam_stream_url")}
    return {"ffmpeg": _find_ffmpeg() or "NAO ENCONTRADO", "impressoras": out}


@app.get("/api/camdebug/{name}")
async def cam_debug(name: str, request: Request):
    if (block := _need_auth(request)):
        return block
    with STATE_LOCK:
        meta = STATE.get(name, {}).get("_meta", {})
        url = meta.get("cam_stream_url")
    return {"name": name, "tem_url": bool(url), "url": url,
            "ffmpeg": _find_ffmpeg() or "não encontrado"}


@app.get("/camera/{name}")
async def camera_stream(name: str, request: Request):
    if (block := _need_auth(request)):
        return block
    cfg = None
    for c in PRINTERS_CFG:
        if c.get("name") == name:
            cfg = c
            break
    if not cfg:
        return JSONResponse({"error": "impressora não encontrada"}, status_code=404)

    brand = cfg.get("brand", "bambu")

    if brand == "anycubic":
        # Snapshot único (o frontend chama em loop) — mais robusto no navegador
        from fastapi.responses import Response
        with STATE_LOCK:
            meta = STATE.get(name, {}).get("_meta", {})
            flv_url = meta.get("cam_stream_url")

        # Se não tem URL ou tem a URL fixa "/flv" (vídeo inativo), ativa o vídeo
        needs_activation = (not flv_url) or flv_url.endswith("/flv")
        if needs_activation:
            holder = PRINTERS.get(name, {}).get("client")
            if isinstance(holder, dict) and holder.get("client"):
                try:
                    vid = {"type": "video", "action": "startCapture",
                           "timestamp": int(time.time() * 1000),
                           "msgid": str(uuid.uuid4()), "data": None}
                    holder["client"].publish(f"{holder['base']}/video", json.dumps(vid))
                    holder["client"].publish(f"{holder['base']}/info", json.dumps(
                        {"type": "info", "action": "query",
                         "timestamp": int(time.time() * 1000),
                         "msgid": str(uuid.uuid4()), "data": None}))
                except Exception:
                    pass
            # espera até 3s a URL dinâmica chegar
            for _ in range(15):
                await asyncio.sleep(0.2)
                with STATE_LOCK:
                    flv_url = STATE.get(name, {}).get("_meta", {}).get("cam_stream_url")
                if flv_url and not flv_url.endswith("/flv"):
                    break

        if not flv_url:
            return JSONResponse({"error": "stream indisponível"}, status_code=503)
        frame = await asyncio.to_thread(_ffmpeg_snapshot, flv_url)
        if not frame:
            return JSONResponse({"error": "sem quadro"}, status_code=503)
        return Response(content=frame, media_type="image/jpeg",
                        headers={"Cache-Control": "no-store"})

    # Bambu: precisa de IP local + access_code (LAN Mode Liveview ativo)
    ip = cfg.get("ip")
    code = cfg.get("access_code")
    if not ip or not code:
        return JSONResponse(
            {"error": "Câmera da Bambu exige IP local e access_code. "
                      "Ative o 'LAN Mode Liveview' na impressora."}, status_code=400)
    return StreamingResponse(
        _ffmpeg_mjpeg(None, is_bambu=True, bambu_ip=ip, bambu_code=code),
        media_type="multipart/x-mixed-replace; boundary=ffmpeg")


def _period_range(period, ref=None):
    """Retorna (start_iso, end_iso, rotulo) para dia/semana/mes/ano."""
    now = ref or datetime.now()
    if period == "dia":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        label = start.strftime("%d/%m/%Y")
    elif period == "semana":
        start = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0)
        label = f"Semana de {start.strftime('%d/%m/%Y')}"
    elif period == "mes":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        label = start.strftime("%m/%Y")
    elif period == "ano":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        label = start.strftime("%Y")
    else:  # tudo
        return None, None, "Todo o período"
    # converte para UTC iso (o banco guarda em UTC)
    start_utc = start.astimezone(timezone.utc).isoformat()
    return start_utc, None, label


def _build_report(period, printers):
    """Monta os dados agregados do relatório."""
    start_iso, end_iso, label = _period_range(period)
    jobs = history_db.query(start_iso, end_iso, printers or None)

    # Agregação por impressora
    by_printer = {}
    for j in jobs:
        pr = j["printer"]
        d = by_printer.setdefault(pr, {
            "printer": pr, "brand": j.get("brand"),
            "total": 0, "success": 0, "failed": 0, "duration_sec": 0, "jobs": []})
        d["total"] += 1
        if j["result"] == "success":
            d["success"] += 1
        else:
            d["failed"] += 1
        d["duration_sec"] += (j.get("duration_sec") or 0)
        d["custo"] = d.get("custo", 0) + (j.get("custo") or 0)
        d["peso_g"] = d.get("peso_g", 0) + (j.get("peso_g") or 0)
        d["jobs"].append(j)

    total = len(jobs)
    success = sum(1 for j in jobs if j["result"] == "success")
    failed = total - success
    total_dur = sum(j.get("duration_sec") or 0 for j in jobs)
    total_custo = sum(j.get("custo") or 0 for j in jobs)
    total_peso = sum(j.get("peso_g") or 0 for j in jobs)
    com_custo = sum(1 for j in jobs if j.get("custo"))

    # Série temporal: impressões por dia (para o gráfico de produção)
    from collections import defaultdict, Counter
    por_dia = defaultdict(lambda: {"total": 0, "success": 0, "failed": 0})
    for j in jobs:
        dia = str(j.get("finished_at", ""))[:10]
        if not dia:
            continue
        por_dia[dia]["total"] += 1
        if j["result"] == "success":
            por_dia[dia]["success"] += 1
        else:
            por_dia[dia]["failed"] += 1
    serie = [{"dia": d, **v} for d, v in sorted(por_dia.items())]

    # Distribuição por material (peso e contagem)
    mats = defaultdict(lambda: {"peso_g": 0.0, "count": 0})
    for j in jobs:
        m = (j.get("material") or "").strip()
        if not m:
            continue
        mats[m]["peso_g"] += (j.get("peso_g") or 0)
        mats[m]["count"] += 1
    materiais = [{"material": m, **v} for m, v in
                 sorted(mats.items(), key=lambda x: -x[1]["peso_g"])]

    # Destaques / ranking
    destaques = {}
    if by_printer:
        mais_prod = max(by_printer.values(), key=lambda d: d["total"])
        destaques["mais_produtiva"] = {"nome": mais_prod["printer"],
                                       "total": mais_prod["total"]}
        # melhor taxa (mínimo 3 impressões para ser justo)
        elegiveis = [d for d in by_printer.values() if d["total"] >= 3]
        if elegiveis:
            melhor = max(elegiveis, key=lambda d: d["success"] / d["total"])
            destaques["melhor_taxa"] = {
                "nome": melhor["printer"],
                "taxa": round(melhor["success"] / melhor["total"] * 100)}
    if serie:
        pico = max(serie, key=lambda s: s["total"])
        destaques["dia_pico"] = {"dia": pico["dia"], "total": pico["total"]}

    return {
        "period": period, "label": label,
        "total": total, "success": success, "failed": failed,
        "success_rate": round(success / total * 100) if total else None,
        "total_hours": round(total_dur / 3600, 1),
        "total_custo": round(total_custo, 2),
        "total_peso_g": round(total_peso, 1),
        "com_custo": com_custo,
        "by_printer": list(by_printer.values()),
        "serie": serie,
        "materiais": materiais,
        "destaques": destaques,
        "jobs": jobs,
    }


@app.get("/api/report")
async def api_report(request: Request, period: str = "mes", printers: str = ""):
    if (block := _need_auth(request)):
        return block
    plist = [p for p in printers.split(",") if p] if printers else None
    data = await asyncio.to_thread(_build_report, period, plist)
    data["available_printers"] = history_db.all_printers()
    return data


@app.get("/api/report/pdf")
async def api_report_pdf(request: Request, period: str = "mes", printers: str = ""):
    if (block := _need_auth(request)):
        return block
    plist = [p for p in printers.split(",") if p] if printers else None
    data = await asyncio.to_thread(_build_report, period, plist)
    pdf_bytes = await asyncio.to_thread(_render_report_pdf, data)
    if not pdf_bytes:
        return JSONResponse({"error": "Não foi possível gerar o PDF."}, status_code=500)
    from fastapi.responses import Response
    fname = f"relatorio_{data['period']}_{datetime.now().strftime('%Y%m%d')}.pdf"
    return Response(content=pdf_bytes, media_type="application/pdf",
                    headers={"Content-Disposition": f'attachment; filename="{fname}"'})


def _fmt_dur(sec):
    if not sec:
        return "—"
    h = sec // 3600
    m = (sec % 3600) // 60
    if h:
        return f"{h}h {m}min"
    return f"{m}min"


def _render_report_pdf(data):
    """Gera o PDF do relatório usando reportlab."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                        Paragraph, Spacer)
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    except ImportError:
        print("[relatorio] reportlab não instalado")
        return None

    import io
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=18 * mm,
                            bottomMargin=18 * mm, leftMargin=16 * mm,
                            rightMargin=16 * mm)
    styles = getSampleStyleSheet()
    title = ParagraphStyle("t", parent=styles["Title"], fontSize=20,
                           textColor=colors.HexColor("#1a1a2e"))
    sub = ParagraphStyle("s", parent=styles["Normal"], fontSize=10,
                         textColor=colors.HexColor("#666"))
    h2 = ParagraphStyle("h2", parent=styles["Heading2"], fontSize=13,
                        textColor=colors.HexColor("#1a1a2e"), spaceBefore=12)

    elems = []
    elems.append(Paragraph("3DWORK · Relatório de Impressões", title))
    elems.append(Paragraph(f"Período: {data['label']} · "
                           f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}", sub))
    elems.append(Spacer(1, 10 * mm))

    # Resumo geral
    resumo = [
        ["Total de impressões", str(data["total"])],
        ["Concluídas com sucesso", str(data["success"])],
        ["Falhas", str(data["failed"])],
        ["Taxa de sucesso", f"{data['success_rate']}%" if data['success_rate'] is not None else "—"],
        ["Horas de impressão", f"{data['total_hours']}h"],
        ["Filamento usado", f"{data.get('total_peso_g', 0):.0f} g"],
        ["Custo total", f"R$ {data.get('total_custo', 0):.2f}"],
    ]
    t = Table(resumo, colWidths=[70 * mm, 40 * mm])
    t.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#555")),
        ("FONTNAME", (1, 0), (1, -1), "Helvetica-Bold"),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LINEBELOW", (0, 0), (-1, -2), 0.5, colors.HexColor("#eee")),
    ]))
    elems.append(Paragraph("Resumo geral", h2))
    elems.append(t)

    # Por impressora
    elems.append(Paragraph("Por impressora", h2))
    header = ["Impressora", "Total", "Sucesso", "Falhas", "Taxa", "Tempo", "Custo"]
    rows = [header]
    for d in sorted(data["by_printer"], key=lambda x: -x["total"]):
        rate = round(d["success"] / d["total"] * 100) if d["total"] else 0
        rows.append([d["printer"], str(d["total"]), str(d["success"]),
                     str(d["failed"]), f"{rate}%", _fmt_dur(d["duration_sec"]),
                     f"R$ {d.get('custo', 0):.2f}"])
    if len(rows) == 1:
        rows.append(["(sem dados no período)", "", "", "", "", "", ""])
    t2 = Table(rows, colWidths=[46 * mm, 16 * mm, 19 * mm, 17 * mm, 15 * mm, 25 * mm, 24 * mm])
    t2.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a1a2e")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f6f6fa")]),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e0e0e8")),
        ("ALIGN", (1, 0), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    elems.append(t2)

    # Detalhe dos jobs (últimos 40)
    if data["jobs"]:
        elems.append(Paragraph("Impressões detalhadas", h2))
        jheader = ["Data", "Impressora", "Arquivo", "Configuração", "Resultado", "Tempo", "Material", "Custo"]
        jrows = [jheader]
        for j in data["jobs"][:40]:
            fin = str(j.get("finished_at", ""))[:16].replace("T", " ")
            res = "✓ Sucesso" if j["result"] == "success" else "✗ Falha"
            custo = j.get("custo")
            mat = j.get("material") or "—"
            peso = j.get("peso_g")
            mat_txt = f"{mat} {peso:.0f}g" if peso else mat
            jrows.append([fin, j["printer"], (j.get("file") or "—")[:20],
                          (j.get("config") or "—")[:18],
                          res, _fmt_dur(j.get("duration_sec")), mat_txt,
                          f"R$ {custo:.2f}" if custo else "—"])
        t3 = Table(jrows, colWidths=[24 * mm, 26 * mm, 28 * mm, 24 * mm,
                                     17 * mm, 15 * mm, 19 * mm, 17 * mm])
        t3.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4f4f6a")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f6f6fa")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e0e0e8")),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        elems.append(t3)

    try:
        doc.build(elems)
        return buf.getvalue()
    except Exception as exc:
        print(f"[relatorio] erro ao gerar PDF: {exc}")
        return None


@app.get("/stats")
async def stats(request: Request):
    if not is_authed(request):
        return JSONResponse({"enabled": False}, status_code=401)
    return await asyncio.to_thread(supabase_logger.stats)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    if not session_valid(ws.cookies.get(SESSION_COOKIE)):
        await ws.close(code=1008)
        return
    await ws.accept()
    broadcaster.clients.add(ws)
    await ws.send_text(broadcaster.snapshot())  # estado inicial
    try:
        while True:
            await ws.receive_text()  # mantém a conexão viva
    except WebSocketDisconnect:
        broadcaster.clients.discard(ws)


# ---------------------------------------------------------------------------
# Detecção pela conta na nuvem + gerenciamento de impressoras (via UI)
# ---------------------------------------------------------------------------
BAMBU_API = {"us": "https://api.bambulab.com", "cn": "https://api.bambulab.cn"}
BAMBU_HEADERS = {"User-Agent": "bambu-dashboard/1.0",
                 "Accept": "application/json", "Content-Type": "application/json"}

def _get_cookie_dbs():
    """Retorna os caminhos dos bancos de cookies por navegador."""
    home = Path.home()
    return {
        "Chrome": home / "AppData" / "Local" / "Google" / "Chrome" / "User Data" / "Default" / "Network" / "Cookies",
        "Edge":   home / "AppData" / "Local" / "Microsoft" / "Edge" / "User Data" / "Default" / "Network" / "Cookies",
        "Brave":  home / "AppData" / "Local" / "BraveSoftware" / "Brave-Browser" / "User Data" / "Default" / "Network" / "Cookies",
    }

_MW_DOMAINS = ("makerworld.com", ".makerworld.com", "www.makerworld.com")


def _dpapi_decrypt(ciphertext: bytes) -> bytes:
    """Descriptografa com DPAPI (Windows). Retorna bytes ou lança exceção."""
    import ctypes
    import ctypes.wintypes

    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_char))]

    p = ctypes.create_string_buffer(ciphertext, len(ciphertext))
    blobin = DATA_BLOB(ctypes.sizeof(p), p)
    blobout = DATA_BLOB()
    retval = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(blobin), None, None, None, None, 0, ctypes.byref(blobout))
    if not retval:
        raise RuntimeError("DPAPI falhou")
    result = ctypes.string_at(blobout.pbData, blobout.cbData)
    ctypes.windll.kernel32.LocalFree(blobout.pbData)
    return result


def _chrome_decrypt(encrypted: bytes, key: bytes) -> str:
    """Descriptografa cookie do Chrome com AES-GCM (v10/v20) ou DPAPI legado."""
    if _HAS_CRYPTOGRAPHY:
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            if encrypted[:3] in (b"v10", b"v20"):
                nonce = encrypted[3:15]
                return AESGCM(key).decrypt(nonce, encrypted[15:], None).decode()
        except Exception:
            pass
    # fallback: DPAPI puro (cookies antigos)
    try:
        return _dpapi_decrypt(encrypted).decode()
    except Exception:
        return ""


def _get_chrome_aes_key(browser_path: Path) -> bytes | None:
    """Lê e descriptografa a chave AES do Local State do Chrome/Edge."""
    try:
        # O Local State fica dois níveis acima do arquivo Cookies
        # Cookies: .../User Data/Default/Network/Cookies
        # Local State: .../User Data/Local State
        local_state = browser_path.parent.parent.parent / "Local State"
        print(f"[cookie] procurando Local State em: {local_state}")
        if not local_state.exists():
            # Tenta um nível acima
            local_state = browser_path.parent.parent.parent.parent / "Local State"
            print(f"[cookie] tentando: {local_state}")
        if not local_state.exists():
            print(f"[cookie] Local State não encontrado")
            return None
        data = json.loads(local_state.read_text(encoding="utf-8"))
        enc_key_b64 = data.get("os_crypt", {}).get("encrypted_key")
        if not enc_key_b64:
            print(f"[cookie] chave encrypted_key não encontrada no Local State")
            return None
        enc_key = base64.b64decode(enc_key_b64)
        if enc_key[:5] != b"DPAPI":
            print(f"[cookie] prefixo DPAPI não encontrado")
            return None
        key = _dpapi_decrypt(enc_key[5:])
        print(f"[cookie] chave AES obtida com sucesso ({len(key)} bytes)")
        return key
    except Exception as e:
        print(f"[cookie] erro ao obter chave AES: {e}")
        return None


def _read_chrome_cookie(db_path: Path, aes_key: bytes | None) -> str:
    """Lê o cookie 'token' do MakerWorld do banco SQLite do Chrome/Edge."""
    import sqlite3, shutil, tempfile
    if not db_path.exists():
        return ""
    # Copia o arquivo — necessário mesmo com Chrome aberto (arquivo bloqueado)
    tmp = Path(tempfile.mktemp(suffix=".db"))
    try:
        # Tenta cópia normal primeiro
        try:
            shutil.copy2(db_path, tmp)
        except Exception:
            # Chrome aberto: tenta via leitura binária direta
            try:
                with open(db_path, "rb") as f:
                    tmp.write_bytes(f.read())
            except Exception:
                return ""
        conn = sqlite3.connect(f"file:{tmp}?mode=ro&immutable=1", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            rows = cur.execute(
                "SELECT name, encrypted_value, host_key FROM cookies "
                "WHERE (host_key LIKE '%makerworld%' OR host_key LIKE '%bambulab%') "
                "AND name = 'token' ORDER BY length(encrypted_value) DESC"
            ).fetchall()
            print(f"[cookie] tokens encontrados: {[(r['name'], r['host_key']) for r in rows]}")
        except Exception as e:
            print(f"[cookie] erro na query: {e}")
            rows = []
        conn.close()
        for row in rows:
            raw = bytes(row["encrypted_value"])
            if not raw:
                continue
            if aes_key:
                val = _chrome_decrypt(raw, aes_key)
            else:
                try:
                    val = _dpapi_decrypt(raw).decode()
                except Exception:
                    val = ""
            if val and len(val) > 20:
                return val
    except Exception:
        pass
    finally:
        try:
            tmp.unlink()
        except Exception:
            pass
    return ""


def _read_firefox_cookie() -> str:
    """Lê o cookie 'token' do MakerWorld do Firefox (sem criptografia extra)."""
    import sqlite3, shutil, tempfile, glob as _glob
    profiles_root = Path.home() / "AppData/Roaming/Mozilla/Firefox/Profiles"
    if not profiles_root.exists():
        return ""
    for cookies_db in profiles_root.glob("*/cookies.sqlite"):
        tmp = Path(tempfile.mktemp(suffix=".db"))
        try:
            shutil.copy2(cookies_db, tmp)
            conn = sqlite3.connect(str(tmp))
            rows = conn.execute(
                "SELECT value FROM moz_cookies WHERE host IN (?, ?) AND name = 'token'",
                _MW_DOMAINS
            ).fetchall()
            conn.close()
            for row in rows:
                val = row[0]
                if val and len(val) > 20:
                    return val
        except Exception:
            pass
        finally:
            try:
                tmp.unlink()
            except Exception:
                pass
    return ""


def _read_makerworld_cookie() -> dict:
    """
    Tenta ler o token do MakerWorld de Chrome, Edge, Brave ou Firefox.
    """
    cookie_dbs = _get_cookie_dbs()
    for browser, db_path in cookie_dbs.items():
        if not db_path.exists():
            print(f"[cookie] {browser}: arquivo não encontrado em {db_path}")
            continue
        print(f"[cookie] {browser}: tentando {db_path}")
        aes_key = _get_chrome_aes_key(db_path)
        print(f"[cookie] {browser}: chave AES {'obtida' if aes_key else 'não obtida'}")
        token = _read_chrome_cookie(db_path, aes_key)
        if token:
            uid = _bambu_uid_from_api(token, "us") or _bambu_uid_from_token(token)
            print(f"[cookie] {browser}: token encontrado! uid={uid}")
            return {"ok": True, "token": token, "uid": uid,
                    "browser": browser, "region": "us"}
        print(f"[cookie] {browser}: token não encontrado")

    # Tenta Firefox
    token = _read_firefox_cookie()
    if token:
        uid = _bambu_uid_from_api(token, "us") or _bambu_uid_from_token(token)
        return {"ok": True, "token": token, "uid": uid,
                "browser": "Firefox", "region": "us"}

    return {"ok": False, "error":
            "Nenhum token encontrado. Certifique-se de estar logado no MakerWorld "
            "e tente a aba 'Token manual' se o problema persistir."}


def _bambu_uid_from_api(token, region):
    """Busca o uid real da conta pela API da Bambu (método confiável)."""
    if not requests:
        return ""
    base = BAMBU_API.get(region, BAMBU_API["us"])
    try:
        r = requests.get(f"{base}/v1/design-user-service/my/preference",
                         headers={**BAMBU_HEADERS, "Authorization": f"Bearer {token}"}, timeout=20)
        if r.status_code == 200:
            uid = r.json().get("uid")
            if uid:
                return f"u_{uid}"
    except Exception:
        pass
    return ""


def _bambu_uid_from_token(token):
    try:
        p = token.split(".")[1]
        p += "=" * (-len(p) % 4)
        claims = json.loads(base64.urlsafe_b64decode(p))
        if claims.get("username"):
            return claims["username"]
        if claims.get("user_id"):
            return f"u_{claims['user_id']}"
    except Exception:
        pass
    return ""


def _bambu_login(email, password, region, code=None):
    if not requests:
        return {"error": "Dependência 'requests' não instalada no servidor."}
    base = BAMBU_API.get(region, BAMBU_API["us"])
    url = f"{base}/v1/user-service/user/login"
    try:
        if code:
            r = requests.post(url, json={"account": email, "code": code}, headers=BAMBU_HEADERS, timeout=20)
        else:
            r = requests.post(url, json={"account": email, "password": password, "apiError": ""}, headers=BAMBU_HEADERS, timeout=20)
    except Exception as exc:
        return {"error": f"Erro de conexão: {exc}"}
    if r.status_code >= 400:
        return {"error": f"Falha no login ({r.status_code}). Verifique os dados."}
    d = r.json()
    if d.get("accessToken") and not d.get("loginType"):
        return {"token": d["accessToken"]}
    if d.get("tfaKey"):
        return {"error": "Conta com autenticador (TFA). Use a opção de token manual."}
    return {"need_code": True}


def _bambu_devices(token, region):
    if not requests:
        return []
    base = BAMBU_API.get(region, BAMBU_API["us"])
    try:
        r = requests.get(f"{base}/v1/iot-service/api/user/bind",
                         headers={**BAMBU_HEADERS, "Authorization": f"Bearer {token}"}, timeout=20)
        if r.status_code >= 400:
            return []
        return r.json().get("devices", [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Adapter Anycubic — MODO LAN (local, tempo real via MQTT da impressora)
# Handshake: GET /info -> POST /ctrl assinado -> descriptografa AES -> MQTT.
# Baseado na engenharia reversa do protocolo LAN da Kobra (firmware de fábrica).
# ---------------------------------------------------------------------------
def _anycubic_lan_sign(token, ts, nonce):
    """sign = md5(md5(token[:16]) + ts + nonce), com duplo url-encode."""
    first = hashlib.md5(token[:16].encode()).hexdigest()
    second = hashlib.md5((first + str(ts) + nonce).encode()).hexdigest()
    return urllib.parse.quote(urllib.parse.quote(second, safe=""))


def _anycubic_lan_decrypt(encrypted_data, token, local_token):
    """Descriptografa a resposta do /ctrl (AES-CBC, chave=token[16:32], IV=local_token)."""
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
    except ImportError:
        try:
            from Cryptodome.Cipher import AES
            from Cryptodome.Util.Padding import unpad
        except ImportError:
            print("[anycubic] pycryptodome não instalado — modo LAN indisponível.")
            return None
    key = token[16:32].encode("utf-8")
    iv = local_token.encode("utf-8")
    iv = iv + (b"\0" * (16 - len(iv))) if len(iv) < 16 else iv[:16]
    ct = base64.b64decode(encrypted_data)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return json.loads(unpad(cipher.decrypt(ct), AES.block_size).decode("utf-8"))


def _local_subnets():
    """Descobre as faixas de rede local do PC (ex: 192.168.1.x)."""
    subnets = []
    try:
        import socket as _s
        # pega o IP local "de saída" (o da interface ativa)
        sock = _s.socket(_s.AF_INET, _s.SOCK_DGRAM)
        sock.settimeout(1)
        try:
            sock.connect(("8.8.8.8", 80))
            local_ip = sock.getsockname()[0]
        finally:
            sock.close()
        parts = local_ip.split(".")
        if len(parts) == 4:
            subnets.append(".".join(parts[:3]))
    except Exception:
        pass
    # fallbacks comuns em redes domésticas
    for fb in ("192.168.1", "192.168.0"):
        if fb not in subnets:
            subnets.append(fb)
    return subnets[:2]   # no máximo 2 faixas, pra não demorar


def _probe_anycubic(ip, timeout=1.2):
    """Testa se há uma Anycubic em modo LAN nesse IP. Retorna dict ou None."""
    if not requests:
        return None
    try:
        r = requests.get(f"http://{ip}:18910/info", timeout=timeout)
        if r.status_code != 200:
            return None
        info = r.json()
        if not info.get("token"):
            return None
        return {
            "ip": ip,
            "name": info.get("printerName") or info.get("name") or f"Kobra {ip}",
            "model": info.get("modelName") or info.get("model") or "Anycubic",
            "serial": ip,
            "printer_id": info.get("printerId") or info.get("deviceId") or ip,
            "online": True,
            "lan_ok": info.get("ctrlType") != "cloud",
        }
    except Exception:
        return None


def scan_anycubic_network():
    """
    Varre a rede local procurando impressoras Anycubic em modo LAN.
    Testa a porta 18910 (a que a Kobra usa no modo LAN) em paralelo.
    """
    from concurrent.futures import ThreadPoolExecutor
    found = []
    ips = []
    for sub in _local_subnets():
        ips.extend(f"{sub}.{i}" for i in range(1, 255))

    with ThreadPoolExecutor(max_workers=80) as pool:
        for res in pool.map(_probe_anycubic, ips):
            if res:
                found.append(res)
    return found


def anycubic_lan_handshake(ip):
    """
    Faz o handshake completo com a Kobra em modo LAN.
    Retorna dict com broker_host, broker_port, username, password,
    device_id, mode_id, model_name — ou None se falhar.
    """
    if not requests:
        return None
    try:
        r = requests.get(f"http://{ip}:18910/info", timeout=6)
        if r.status_code != 200:
            return None
        info = r.json()
        token = info.get("token")
        ctrl_url = info.get("ctrlInfoUrl")
        if not token or not ctrl_url:
            return None
        if info.get("ctrlType") == "cloud":
            print(f"[anycubic {ip}] impressora em modo NUVEM — ative o Modo LAN.")
            return None

        ts = int(time.time() * 1000)
        nonce = "".join(random.choices(string.ascii_letters + string.digits, k=6))
        did = "".join(random.choices(string.ascii_uppercase + string.digits, k=32))
        sign = _anycubic_lan_sign(token, ts, nonce)
        cr = requests.post(ctrl_url,
                           params={"ts": ts, "nonce": nonce, "sign": sign, "did": did},
                           timeout=6)
        if cr.status_code != 200:
            return None
        cd = cr.json()
        if cd.get("code") != 200:
            return None
        local_token = cd["data"]["token"]
        pdata = _anycubic_lan_decrypt(cd["data"]["info"], token, local_token)
        if not pdata:
            return None

        m = re.match(r"mqtts?://([^:]+):(\d+)", pdata.get("broker", ""))
        if not m:
            return None
        return {
            "broker_host": m.group(1),
            "broker_port": int(m.group(2)),
            "username": pdata.get("username"),
            "password": pdata.get("password"),
            "device_id": pdata.get("deviceId"),
            "mode_id": str(pdata.get("modeId") or info.get("modelId")),
            "model_name": pdata.get("modelName") or info.get("modelName") or "Anycubic",
            "printer_name": info.get("deviceName") or pdata.get("modelName") or "Anycubic",
            "cn": info.get("cn"),
        }
    except Exception as exc:
        print(f"[anycubic {ip}] handshake falhou: {exc}")
        return None


# ---------------------------------------------------------------------------
# Adapter Anycubic (nuvem, via polling da API REST com o token do site)
# O token é o "XX-Token" do localStorage de cloud-universe.anycubic.com.
# A nuvem e consultada a cada ANYCUBIC_POLL segundos (padrao 15s).
# ---------------------------------------------------------------------------
ANYCUBIC_API = "https://cloud-universe.anycubic.com"
ANYCUBIC_POLL = 15  # segundos entre consultas

# Estados da Anycubic -> nosso formato (mesma linguagem que a Bambu usa)
_ANYCUBIC_STATE_MAP = {
    "printing": "RUNNING",
    "paused": "PAUSE",
    "pausing": "PAUSE",
    "finished": "FINISH",
    "completed": "FINISH",
    "failed": "FAILED",
    "stopped": "IDLE",
    "idle": "IDLE",
    "free": "IDLE",
    "offline": "IDLE",
    "busy": "RUNNING",
    "heating": "PREPARE",
    "preparing": "PREPARE",
    "leveling": "PREPARE",
}


def _anycubic_headers(token):
    return {
        "XX-Token": token,
        "XX-Device-Type": "web",
        "User-Agent": "hub3d/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _anycubic_get(path, token, params=None):
    """GET num endpoint da nuvem Anycubic. Retorna o campo 'data' ou None."""
    if not requests:
        return None
    try:
        r = requests.get(f"{ANYCUBIC_API}{path}", headers=_anycubic_headers(token),
                         params=params, timeout=20)
        if r.status_code >= 400:
            return None
        body = r.json()
        if isinstance(body, dict):
            if body.get("code") not in (200, 0, None):
                return None
            return body.get("data", body)
        return body
    except Exception:
        return None


def anycubic_list_printers(token):
    """Lista as impressoras da conta. Retorna lista de dicts crus da API."""
    data = _anycubic_get("/work/printer/getPrinters", token)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("list") or data.get("printers") or []
    return []


def anycubic_printers_status(token):
    """Status de todas as impressoras da conta (endpoint leve p/ polling)."""
    data = _anycubic_get("/work/printer/printersStatus", token)
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return data.get("list") or data.get("printers") or []
    return []


def anycubic_current_project(token, printer_id):
    """Trabalho de impressao atual de uma impressora (progresso, tempos)."""
    data = _anycubic_get("/v2/project/monitor", token, params={"id": printer_id})
    if data is None:
        data = _anycubic_get("/work/project/getProjects", token,
                             params={"printer_id": printer_id})
    return data


def _num(v):
    """Converte com seguranca para numero (a API as vezes manda string)."""
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def anycubic_translate_lan(data):
    """
    Traduz o 'data' de um report LAN da Kobra para o formato padrão do painel.
    Formato real observado: data.state, data.temp{curr_nozzle_temp,...},
    data.project{...} quando imprimindo.
    Retorna None se não houver nada útil.
    """
    if not isinstance(data, dict):
        return None

    out = {}
    got = False

    # Estado
    state = data.get("state")
    if state is not None:
        out["gcode_state"] = _ANYCUBIC_STATE_MAP.get(str(state).lower(), "IDLE")
        got = True

    # Temperaturas
    temp = data.get("temp") or {}
    if temp:
        n = _num(temp.get("curr_nozzle_temp"))
        b = _num(temp.get("curr_hotbed_temp"))
        if n is not None:
            out["nozzle_temper"] = n
        if b is not None:
            out["bed_temper"] = b
        got = True

    # Trabalho de impressão (quando existe)
    proj = data.get("project")
    if isinstance(proj, dict):
        prog = _num(proj.get("progress"))
        if prog is not None:
            out["mc_percent"] = int(prog * 100) if prog <= 1 else int(prog)
        remain = _num(proj.get("remain_time") or proj.get("left_time"))
        if remain is not None:
            # a Kobra manda em segundos
            out["mc_remaining_time"] = int(remain / 60) if remain > 600 else int(remain)
        cl = _num(proj.get("curr_layer") or proj.get("print_layer"))
        tl = _num(proj.get("total_layer") or proj.get("total_layers"))
        if cl is not None:
            out["layer_num"] = int(cl)
        if tl is not None:
            out["total_layer_num"] = int(tl)
        fname = (proj.get("name") or proj.get("filename") or
                 proj.get("print_name") or proj.get("file_name"))
        if fname:
            out["subtask_name"] = fname
            out["gcode_file"] = fname
        got = True
    elif state and str(state).lower() in ("free", "idle", "offline"):
        # ocioso: zera progresso
        out["mc_percent"] = 0
        out["mc_remaining_time"] = None

    # Caixa multicolor (ACE Pro) → formato 'ams' que o card já desenha
    mcb = data.get("multi_color_box")
    if isinstance(mcb, list) and mcb:
        ams_units = []
        active_global = -1
        for bi, box in enumerate(mcb):
            loaded = box.get("loaded_slot", -1)
            trays = []
            for slot in box.get("slots", []):
                idx = slot.get("index", 0)
                rgb = slot.get("color") or [58, 65, 80]
                if isinstance(rgb, list) and len(rgb) == 3:
                    hexcol = f"{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}FF"
                else:
                    hexcol = "3A4150FF"
                mat = slot.get("type") or ""
                has = str(slot.get("status", 0)) not in ("0", "", "None")
                trays.append({
                    "id": str(idx),
                    "tray_color": hexcol,
                    "tray_type": mat if mat and mat.lower() != "unknown" else ("PLA" if has else ""),
                })
                if loaded == idx and active_global < 0:
                    active_global = bi * 4 + idx
            ams_units.append({"id": str(bi), "tray": trays})
        if ams_units:
            out["ams"] = {"ams": ams_units,
                          "tray_now": str(active_global if active_global >= 0 else 0)}
            got = True

    return out if got else None


def anycubic_translate(printer, project):
    """
    Traduz o status cru da Anycubic para o nosso formato padrao
    ('print' com os mesmos campos que a Bambu usa), pro painel nao
    precisar saber a marca.
    """
    p = printer or {}
    j = project or {}

    raw_state = str(p.get("print_status") or p.get("status") or
                    p.get("printStatus") or "idle").lower()
    gcode_state = _ANYCUBIC_STATE_MAP.get(raw_state, "IDLE")

    progress = _num(j.get("progress") or p.get("progress") or
                    j.get("print_progress"))
    if progress is not None and progress <= 1:
        progress = progress * 100

    remain_s = _num(j.get("remain_time") or j.get("left_time") or
                    j.get("remaining_time") or p.get("remain_time"))
    remain_min = int(remain_s / 60) if remain_s is not None else None

    nozzle = _num(p.get("curr_nozzle_temp") or p.get("nozzle_temp") or
                  p.get("hotend_temp") or j.get("curr_nozzle_temp"))
    bed = _num(p.get("curr_hotbed_temp") or p.get("hotbed_temp") or
               p.get("bed_temp") or j.get("curr_hotbed_temp"))

    cur_layer = _num(j.get("curr_layer") or p.get("curr_layer"))
    total_layer = _num(j.get("total_layer") or p.get("total_layer"))

    fname = (j.get("name") or j.get("file_name") or j.get("model_name") or
             p.get("print_name") or "")

    out = {
        "gcode_state": gcode_state,
        "mc_percent": int(progress) if progress is not None else 0,
        "mc_remaining_time": remain_min,
        "nozzle_temper": nozzle,
        "bed_temper": bed,
    }
    if cur_layer is not None:
        out["layer_num"] = int(cur_layer)
    if total_layer is not None:
        out["total_layer_num"] = int(total_layer)
    if fname:
        out["subtask_name"] = fname
        out["gcode_file"] = fname
    return out


def _detect_bambu(body):
    region = body.get("region", "us")
    token = (body.get("token") or "").strip()
    uid = (body.get("uid") or "").strip()
    if not token:
        res = _bambu_login(body.get("email", ""), body.get("password", ""),
                           region, body.get("code"))
        if res.get("error"):
            return {"ok": False, "error": res["error"]}
        if res.get("need_code"):
            return {"ok": False, "need_code": True}
        token = res["token"]
    if not uid:
        # Prioridade: API (confiável) → decodificação do token (fallback)
        uid = _bambu_uid_from_api(token, region) or _bambu_uid_from_token(token)
    devices = _bambu_devices(token, region)
    printers = [{
        "serial": d.get("dev_id"),
        "name": d.get("name") or d.get("dev_id"),
        "model": d.get("dev_product_name") or d.get("dev_model_name") or "",
        "online": bool(d.get("online")),
    } for d in devices if d.get("dev_id")]
    return {"ok": True, "token": token, "uid": uid, "region": region, "printers": printers}


def _need_auth(request):
    return None if is_authed(request) else JSONResponse(
        {"ok": False, "error": "Não autenticado."}, status_code=401)


@app.post("/api/detect")
async def api_detect(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    brand = body.get("brand", "bambu")
    if brand == "bambu":
        return await asyncio.to_thread(_detect_bambu, body)
    if brand == "anycubic":
        return await asyncio.to_thread(_detect_anycubic, body)
    return {"ok": False, "error": "Essa marca ainda não está disponível (em breve)."}


def _detect_anycubic(body):
    # Modo BUSCA AUTOMÁTICA: varre a rede local
    if body.get("scan"):
        found = scan_anycubic_network()
        if not found:
            return {"ok": False, "error":
                    "Nenhuma impressora encontrada na rede. Verifique se o "
                    "Modo LAN está ativo na impressora e se ela está na mesma "
                    "rede do computador. Você também pode digitar o IP manualmente."}
        # avisa se achou alguma que está em modo nuvem
        prontas = [f for f in found if f.get("lan_ok")]
        if not prontas:
            return {"ok": False, "error":
                    "Encontrei impressora(s) na rede, mas em modo NUVEM. "
                    "Ative o Modo LAN na tela da impressora e tente de novo."}
        return {"ok": True, "printers": [
            {"serial": f["ip"], "ip": f["ip"], "printer_id": f["printer_id"],
             "name": f["name"], "model": f["model"], "online": True}
            for f in prontas]}

    # Modo MANUAL: IP digitado
    ip = (body.get("ip") or body.get("token") or "").strip()
    if not ip:
        return {"ok": False, "error": "Informe o IP da impressora (ex.: 192.168.1.15)."}
    # valida formato básico de IP
    if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip):
        return {"ok": False, "error": "IP inválido. Use algo como 192.168.1.15."}
    hs = anycubic_lan_handshake(ip)
    if not hs:
        return {"ok": False, "error":
                "Não consegui conectar. Verifique se o Modo LAN está ativo na "
                "impressora e se o IP está certo (mesma rede do computador)."}
    return {"ok": True, "printers": [{
        "serial": ip,
        "ip": ip,
        "printer_id": hs.get("device_id"),
        "name": hs.get("printer_name") or hs.get("model_name") or f"Anycubic {ip}",
        "model": hs.get("model_name") or "Kobra",
        "online": True,
    }]}


@app.get("/api/bambu/autodetect")
async def api_bambu_autodetect(request: Request):
    """Tenta ler o token do MakerWorld do disco (cookie do navegador)."""
    if (block := _need_auth(request)):
        return block
    result = await asyncio.to_thread(_read_makerworld_cookie)
    if not result["ok"]:
        return JSONResponse(result, status_code=404)
    token = result["token"]
    devices = await asyncio.to_thread(_bambu_devices, token, result.get("region", "us"))
    printers = [{
        "serial": d.get("dev_id"),
        "name": d.get("name") or d.get("dev_id"),
        "model": d.get("dev_product_name") or d.get("dev_model_name") or "",
        "online": bool(d.get("online")),
    } for d in devices if d.get("dev_id")]
    return {**result, "printers": printers}


@app.get("/api/bambu/cookiediag")
async def api_cookie_diag(request: Request):
    """Diagnóstico: mostra quais bancos de cookies existem e o status."""
    if (block := _need_auth(request)):
        return block
    diag = {"cryptography": _HAS_CRYPTOGRAPHY, "browsers": {}}
    for browser, db_path in _get_cookie_dbs().items():
        diag["browsers"][browser] = str(db_path) if db_path.exists() else "não encontrado"
    ff_root = Path.home() / "AppData" / "Roaming" / "Mozilla" / "Firefox" / "Profiles"
    ff_dbs = list(ff_root.glob("*/cookies.sqlite")) if ff_root.exists() else []
    diag["browsers"]["Firefox"] = str(ff_dbs[0]) if ff_dbs else "não encontrado"
    return diag


@app.post("/api/printer/add")
async def api_add(request: Request):
    if (block := _need_auth(request)):
        return block
    cfg = await request.json()
    cfg.setdefault("brand", "bambu")
    cfg.setdefault("mode", "cloud")
    ok, err = add_printer_cfg(cfg)
    return {"ok": ok, "error": err}


@app.post("/api/printer/remove")
async def api_remove(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    ok, err = remove_printer_cfg(body.get("name", ""))
    return {"ok": ok, "error": err}


@app.post("/api/printer/rename")
async def api_rename(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    name = body.get("name", "")
    apelido = (body.get("apelido", "") or "").strip()[:60]
    found = False
    for c in PRINTERS_CFG:
        if c.get("name") == name:
            if apelido:
                c["apelido"] = apelido
            else:
                c.pop("apelido", None)  # apelido vazio = volta ao nome original
            found = True
            break
    if not found:
        return {"ok": False, "error": "Impressora não encontrada."}
    save_printers(PRINTERS_CFG)
    # reflete no estado ao vivo para o card atualizar
    if name in STATE:
        STATE[name].setdefault("_meta", {})["apelido"] = apelido
    return {"ok": True, "apelido": apelido}


@app.post("/api/printer/reorder")
async def api_reorder(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    ok, err = reorder_printers(body.get("order", []))
    return {"ok": ok, "error": err}


LOGO_DATA_URI = "data:image/png;base64,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"

# Logo personalizável: se existir logo_cliente.png/jpg na pasta, usa ele;
# senão, usa o logo padrão da 3DWORK embutido acima.
_LOGO_CUSTOM_NAMES = ["logo_cliente.png", "logo_cliente.jpg", "logo_cliente.jpeg"]


def get_logo_uri():
    for nome in _LOGO_CUSTOM_NAMES:
        p = Path(__file__).with_name(nome)
        if p.exists():
            try:
                data = p.read_bytes()
                if len(data) > 0:
                    ext = "jpeg" if nome.endswith(("jpg", "jpeg")) else "png"
                    return f"data:image/{ext};base64," + base64.b64encode(data).decode()
            except Exception:
                pass
    return LOGO_DATA_URI


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>3DWORK · Farm de Impressoras</title>
<link rel="icon" href="__LOGO_SRC__">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
<style>
  :root{
    --void:#070a10; --panel:#0d121b; --panel-2:#111826;
    --hair:#1c2535; --hair-lit:#2a3a54;
    --ink:#e9eef7; --muted:#8593a8; --faint:#56627b;
    --live:#4f8cff; --chrome:#cfe0f5;
    --heat:#ff7a3d; --done:#37d399; --warn:#ffcc44; --fail:#ff5470;
  }
  *{box-sizing:border-box}
  html,body{margin:0}
  body{
    background:
      radial-gradient(900px 520px at 82% -14%, #142035 0%, transparent 62%),
      var(--void);
    color:var(--ink);
    font-family:'Space Grotesk',system-ui,sans-serif;
    -webkit-font-smoothing:antialiased; min-height:100vh;
  }
  /* leve textura de mesa de impressão no fundo */
  body::before{
    content:""; position:fixed; inset:0; pointer-events:none; z-index:0;
    background-image:
      linear-gradient(rgba(79,140,255,.035) 1px, transparent 1px),
      linear-gradient(90deg, rgba(79,140,255,.035) 1px, transparent 1px);
    background-size:46px 46px;
    mask-image:radial-gradient(900px 700px at 50% -5%, #000 0%, transparent 75%);
  }
  .wrap{position:relative; z-index:1}

  /* ── Header ───────────────────────────────────────────── */
  header{
    display:flex; align-items:center; justify-content:space-between;
    gap:1rem 1.5rem; padding:1.05rem clamp(1rem,4vw,3rem); background:#ffffff;
    position:relative; box-shadow:0 1px 0 rgba(0,0,0,.08); flex-wrap:wrap;
  }
  header::after{content:""; position:absolute; left:0; right:0; bottom:0;
    height:1px; opacity:.6;
    background:linear-gradient(90deg,transparent,#0077cc 16%,#00AFF0 50%,#0077cc 84%,transparent);}
  .brand{display:flex; align-items:center; gap:1rem; min-width:0; flex:0 0 auto}
  .hdr-right{display:flex; align-items:center; gap:.6rem; flex-wrap:wrap;
    justify-content:flex-end; flex:1 1 auto; min-width:0}
  .brand .logo{height:46px; width:auto; display:block;
    filter:drop-shadow(0 2px 6px rgba(0,0,0,.12))}
  .brand .sep{width:1px; height:30px; background:#d0d8e4; flex:0 0 1px}
  .brand .title{font-weight:500; font-size:1.02rem; letter-spacing:.17em;
    text-transform:uppercase; color:#363435; white-space:nowrap}
  .conn{font-family:'JetBrains Mono',monospace; font-size:.72rem;
    color:#888; display:flex; align-items:center; gap:.5rem; white-space:nowrap}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--fail);
    box-shadow:0 0 9px var(--fail)}
  .dot.live{background:var(--done);box-shadow:0 0 9px var(--done);
    animation:pulse 2.2s ease-in-out infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.45}}

  /* ── Overview da frota ────────────────────────────────── */
  .overview{
    display:flex; gap:1.4rem; flex-wrap:wrap; align-items:center;
    padding:1rem clamp(1rem,4vw,3rem);
    border-bottom:1px solid var(--hair);
    background:linear-gradient(180deg, rgba(20,28,46,.35), transparent);
  }
  .eyebrow{font-family:'JetBrains Mono',monospace; font-size:.62rem;
    letter-spacing:.22em; text-transform:uppercase; color:var(--faint)}
  .fleet{display:flex; flex-direction:column; gap:.55rem; min-width:240px; flex:1}
  .fleet-counts{display:flex; gap:1.2rem; flex-wrap:wrap}
  .fc{display:flex; align-items:baseline; gap:.4rem;
    font-family:'JetBrains Mono',monospace; font-size:.8rem; color:var(--muted)}
  .fc b{font-size:1.05rem; color:var(--ink); font-weight:500}
  .fc .pip{width:8px;height:8px;border-radius:50%;display:inline-block;
    align-self:center}
  .fleet-bar{display:flex; height:7px; border-radius:99px; overflow:hidden;
    background:#161e2c; border:1px solid var(--hair)}
  .fleet-bar i{display:block; height:100%}
  .farm-stats{display:flex; gap:1.4rem; flex-wrap:wrap; align-items:center}
  .fs{display:flex; flex-direction:column; gap:.15rem}
  .fs .k{font-family:'JetBrains Mono',monospace; font-size:.6rem;
    letter-spacing:.12em; text-transform:uppercase; color:var(--faint)}
  .fs .v{font-family:'JetBrains Mono',monospace; font-size:1rem; color:var(--ink)}
  .eta-total{display:flex; flex-direction:column; gap:.15rem; padding-left:.2rem}
  .eta-total .v{color:var(--live)}

  /* ═══════════════════════════════════════════════════════
     LAYOUT COM SIDEBAR
     ═══════════════════════════════════════════════════════ */
  .app{display:flex; min-height:100vh}
  .sidebar{width:160px; flex:0 0 160px; background:#0b1017;
    border-right:1px solid var(--hair); display:flex; flex-direction:column;
    position:sticky; top:0; height:100vh; z-index:50}
  .sb-brand{padding:1rem .5rem .9rem; display:flex; align-items:center;
    justify-content:center; min-height:52px}
  .sb-logo{display:block; width:100%; max-width:150px; height:auto;
    object-fit:contain; margin:0 auto}
  .sb-nav{flex:1; padding:.6rem .45rem; display:flex; flex-direction:column; gap:.2rem}
  .sb-foot{padding:.45rem; border-top:1px solid var(--hair); display:flex;
    flex-direction:column; gap:.2rem}
  .sb-version{text-align:center; font-size:.68rem; color:var(--faint);
    padding:.4rem 0 .2rem; letter-spacing:.03em}
  .sb-item{display:flex; align-items:center; gap:.5rem; width:100%;
    background:transparent; border:0; border-radius:10px; padding:.6rem .55rem;
    cursor:pointer; font-family:inherit; font-size:.82rem; color:var(--muted);
    text-align:left; text-decoration:none; transition:all .15s}
  .sb-item:hover{background:#ffffff0a; color:var(--ink)}
  .sb-item.active{background:rgba(0,122,204,.14); color:var(--live)}
  .sb-item.active .sb-ic{filter:none}
  .sb-ic{width:20px; text-align:center; font-size:1rem; flex:0 0 auto}
  .sb-tx{white-space:nowrap; overflow:hidden; text-overflow:ellipsis}
  .content{flex:1; min-width:0; display:flex; flex-direction:column}
  .topbar{display:flex; align-items:center; gap:1rem; padding:1rem 1.6rem;
    background:#ffffff05; border-bottom:1px solid var(--hair); position:sticky;
    top:0; z-index:40; backdrop-filter:blur(8px)}
  .tb-title{font-size:1.15rem; font-weight:600; flex:1}
  .sb-toggle{display:none; background:transparent; border:0; color:var(--ink);
    font-size:1.3rem; cursor:pointer}
  .page{padding:1.6rem; flex:1}
  .printers-head{display:flex; gap:.6rem; margin-bottom:1.2rem; flex-wrap:wrap; align-items:center}
  .ph-spacer{flex:1; min-width:0}
  #page-printers main{padding:0}
  #page-printers .overview{margin-bottom:1.4rem}

  /* Mobile: sidebar vira gaveta */
  @media (max-width:900px){
    .sidebar{position:fixed; left:0; top:0; transform:translateX(-100%);
      transition:transform .25s; box-shadow:4px 0 24px rgba(0,0,0,.4)}
    .app.sb-open .sidebar{transform:translateX(0)}
    .sb-toggle{display:block}
    .page{padding:1rem}
    .app.sb-open::after{content:""; position:fixed; inset:0; background:rgba(0,0,0,.5);
      z-index:45}
  }

  /* ── Dashboard (nova home) ────────────────────────────── */
  .dash-grid{display:grid; gap:1.2rem}
  .dash-kpis{display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:1rem}
  .kpi{background:linear-gradient(160deg,#111826,#0c1119);
    border:1px solid var(--hair); border-radius:16px; padding:1.2rem 1.3rem;
    position:relative; overflow:hidden}
  .kpi::before{content:""; position:absolute; top:0; left:0; width:100%; height:3px;
    background:var(--accent,#007acc)}
  .kpi.k-live::before{background:#37d67a}
  .kpi.k-cost::before{background:#35d17c}
  .kpi.k-fila::before{background:#e0a94f}
  .kpi.k-ok::before{background:#4f8cff}
  .kpi-ic{font-size:1.3rem; opacity:.9; margin-bottom:.5rem}
  .kpi-v{font-size:2.1rem; font-weight:700; line-height:1; font-family:'JetBrains Mono',monospace}
  .kpi-k{font-size:.78rem; color:var(--muted); margin-top:.45rem; text-transform:uppercase;
    letter-spacing:.06em}
  .kpi-sub{font-size:.74rem; color:var(--faint); margin-top:.3rem}
  .dash-cols{display:grid; grid-template-columns:1.4fr 1fr; gap:1.2rem}
  @media(max-width:1000px){.dash-cols{grid-template-columns:1fr}}
  .dash-box{background:#0c1119; border:1px solid var(--hair); border-radius:16px;
    padding:1.2rem 1.3rem}
  .dash-box h3{font-size:.82rem; text-transform:uppercase; letter-spacing:.08em;
    color:var(--muted); margin:0 0 1rem; font-weight:600}
  .pstat{display:flex; align-items:center; gap:.9rem; padding:.75rem 0;
    border-bottom:1px solid var(--hair)}
  .pstat:last-child{border-bottom:0}
  .pstat-dot{width:10px; height:10px; border-radius:50%; flex:0 0 auto}
  .pstat-dot.on{background:#37d67a; box-shadow:0 0 8px #37d67a}
  .pstat-dot.off{background:#5a6473}
  .pstat-dot.run{background:#4f8cff; box-shadow:0 0 8px #4f8cff}
  .pstat-dot.err{background:#ff5470; box-shadow:0 0 8px #ff5470}
  .pstat-info{flex:1; min-width:0}
  .pstat-name{font-size:.92rem; font-weight:600; white-space:nowrap; overflow:hidden;
    text-overflow:ellipsis}
  .pstat-sub{font-size:.74rem; color:var(--muted)}
  .pstat-metrics{display:flex; gap:1.2rem; flex:0 0 auto}
  .pstat-m{text-align:right}
  .pstat-m b{font-family:'JetBrains Mono',monospace; font-size:.92rem; display:block}
  .pstat-m span{font-size:.66rem; color:var(--faint); text-transform:uppercase}
  .pstat-prog{width:60px; height:5px; background:#1a2333; border-radius:3px;
    overflow:hidden; flex:0 0 auto}
  .pstat-prog i{display:block; height:100%; background:#4f8cff}
  .dash-bars{display:flex; flex-direction:column; gap:.7rem; margin-top:.3rem}
  .dbar{display:flex; align-items:center; gap:.7rem}
  .dbar-name{width:120px; font-size:.8rem; color:var(--muted); white-space:nowrap;
    overflow:hidden; text-overflow:ellipsis; flex:0 0 auto}
  .dbar-track{flex:1; height:22px; background:#0a0e16; border-radius:6px; overflow:hidden;
    position:relative}
  .dbar-fill{height:100%; border-radius:6px; display:flex; align-items:center;
    padding:0 .5rem; font-size:.7rem; font-weight:700; color:#04122e; min-width:2px}
  .dash-period{display:flex; gap:.4rem; flex-wrap:wrap}

  /* ── Banner de atualização ────────────────────────────── */
  .update-banner{display:flex; align-items:center; justify-content:space-between;
    gap:1rem; flex-wrap:wrap; background:linear-gradient(90deg,rgba(53,209,124,.15),rgba(79,140,255,.15));
    border-bottom:1px solid rgba(53,209,124,.35); padding:.75rem 1.6rem;
    font-size:.88rem; color:var(--ink)}
  .update-banner b{color:#37d67a}
  .ub-btns{display:flex; gap:.5rem; flex:0 0 auto}
  .ub-btns button{cursor:pointer; font-family:inherit; font-size:.82rem; font-weight:600;
    border-radius:8px; padding:.45rem .9rem; border:1px solid var(--hair)}
  .ub-later{background:transparent; color:var(--muted)}
  .ub-later:hover{color:var(--ink)}
  .ub-now{background:var(--ac); border-color:var(--ac); color:#fff}
  .ub-now:hover{filter:brightness(1.1)}

  /* ── Destaques / ranking ──────────────────────────────── */
  .hl-grid{display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:1rem}
  .hl-card{display:flex; align-items:center; gap:.9rem; background:linear-gradient(135deg,#141b28,#0d1420);
    border:1px solid var(--hair); border-radius:14px; padding:1rem 1.1rem;
    position:relative; overflow:hidden}
  .hl-card::after{content:""; position:absolute; top:-40%; right:-10%; width:90px; height:180%;
    background:radial-gradient(circle, rgba(79,140,255,.12), transparent 70%)}
  .hl-ic{font-size:1.9rem; flex:0 0 auto}
  .hl-tx{display:flex; flex-direction:column; min-width:0}
  .hl-tx small{font-size:.68rem; text-transform:uppercase; letter-spacing:.08em; color:var(--faint)}
  .hl-tx b{font-size:1.05rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis}
  .hl-tx span{font-size:.78rem; color:var(--muted)}

  /* ── Gráfico de produção ──────────────────────────────── */
  .prod-wrap{overflow-x:auto}
  .prod-bar{transform:scaleY(0); transform-origin:bottom; animation:growBar .5s ease forwards; animation-delay:var(--d,0s)}
  @keyframes growBar{to{transform:scaleY(1)}}
  .prod-legend{display:flex; gap:1.2rem; margin-top:.6rem; font-size:.76rem; color:var(--muted)}
  .prod-legend i{display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:.3rem; vertical-align:middle}

  /* ── Cards de impressora ao vivo ──────────────────────── */
  .pcard-grid{display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:1rem}
  .pcard{display:flex; align-items:center; gap:1rem; background:#0c1119;
    border:1px solid var(--hair); border-radius:14px; padding:1rem;
    transition:border-color .2s}
  .pcard.run{border-color:rgba(79,140,255,.4)}
  .pcard.run .pcard-dot{animation:pulse 1.4s infinite}
  .pcard.err{border-color:rgba(255,84,112,.4)}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
  .pcard-ring{flex:0 0 auto}
  .pcard-arc{transition:stroke-dashoffset 1s cubic-bezier(.4,0,.2,1)}
  .pcard-info{min-width:0; flex:1}
  .pcard-name{font-weight:600; font-size:.98rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis}
  .pcard-status{display:flex; align-items:center; gap:.4rem; font-size:.82rem; margin-top:.15rem}
  .pcard-dot{width:8px; height:8px; border-radius:50%; flex:0 0 auto}
  .pcard-obj{font-size:.76rem; color:var(--muted); margin-top:.35rem; white-space:nowrap;
    overflow:hidden; text-overflow:ellipsis}
  .pcard-time{font-size:.76rem; color:var(--faint); margin-top:.2rem; font-family:'JetBrains Mono',monospace}

  /* ── Gerenciador de projetos ──────────────────────────── */
  .proj-locals{display:flex; gap:.5rem; margin-bottom:1.2rem}
  .proj-tab{background:#0c1119; border:1px solid var(--hair); border-radius:10px;
    padding:.6rem 1.1rem; cursor:pointer; font-family:inherit; font-size:.9rem;
    color:var(--muted); transition:all .15s}
  .proj-tab:hover{color:var(--ink)}
  .proj-tab.active{background:rgba(0,122,204,.14); border-color:var(--live); color:var(--live)}
  .proj-bar{display:flex; align-items:center; justify-content:space-between;
    gap:1rem; margin-bottom:1rem; flex-wrap:wrap}
  .proj-crumbs{display:flex; align-items:center; gap:.3rem; flex-wrap:wrap}
  .crumb{background:transparent; border:0; color:var(--muted); cursor:pointer;
    font-family:inherit; font-size:.86rem; padding:.3rem .4rem; border-radius:6px}
  .crumb:hover{color:var(--ink); background:#ffffff08}
  .crumb-sep{color:var(--faint)}
  .proj-tools{display:flex; gap:.5rem}
  .proj-progress{background:rgba(79,140,255,.1); border:1px solid rgba(79,140,255,.35);
    border-radius:8px; padding:.6rem 1rem; margin-bottom:1rem; color:var(--live);
    font-size:.85rem}
  .proj-grid{display:grid; grid-template-columns:repeat(auto-fill,minmax(180px,1fr)); gap:.9rem}
  .proj-item{background:#0c1119; border:1px solid var(--hair); border-radius:12px;
    padding:1rem; display:flex; flex-direction:column; align-items:center;
    gap:.6rem; position:relative; transition:all .15s; cursor:default}
  .proj-item:hover{border-color:var(--live); background:#111826}
  .proj-folder{cursor:pointer}
  .pi-ic{font-size:2.4rem; line-height:1}
  .pi-ext{width:56px; height:56px; border-radius:12px; display:flex;
    align-items:center; justify-content:center; font-size:1.8rem}
  .ext-stl,.ext-obj{background:rgba(79,140,255,.14)}
  .ext-3mf{background:rgba(53,209,124,.14)}
  .ext-gcode,.ext-g,.ext-gco{background:rgba(224,169,79,.14)}
  .pi-name{font-size:.84rem; text-align:center; word-break:break-word;
    display:flex; flex-direction:column; gap:.15rem; width:100%}
  .pi-size{font-size:.68rem; color:var(--faint)}
  .pi-actions{position:absolute; top:.5rem; right:.5rem; display:flex; gap:.2rem;
    opacity:0; transition:opacity .15s}
  .proj-item:hover .pi-actions{opacity:1}
  .pi-actions button,.pi-actions a{background:#0a0e16; border:1px solid var(--hair);
    border-radius:6px; width:26px; height:26px; display:flex; align-items:center;
    justify-content:center; cursor:pointer; color:var(--muted); font-size:.8rem;
    text-decoration:none}
  .pi-actions button:hover,.pi-actions a:hover{color:var(--live); border-color:var(--live)}
  .proj-cloud-setup{max-width:560px; margin:1rem auto; text-align:center;
    background:#0c1119; border:1px solid var(--hair); border-radius:16px; padding:2rem}
  .pcs-ic{font-size:3rem; margin-bottom:.5rem}
  .proj-cloud-setup h3{margin:0 0 .7rem}
  .proj-cloud-setup p{color:var(--muted); font-size:.88rem; line-height:1.6; margin:0 0 1rem}
  .pcs-ex{font-size:.8rem}
  .pcs-ex code{background:#0a0e16; padding:.2rem .4rem; border-radius:4px;
    color:var(--live); font-size:.78rem}
  .pcs-row{display:flex; gap:.5rem; margin:1.2rem 0}
  .pcs-row input{flex:1; background:#0a0e16; border:1px solid var(--hair);
    border-radius:8px; padding:.6rem .8rem; color:var(--ink); font-family:inherit; font-size:.85rem}
  .pcs-row input:focus{outline:none; border-color:var(--live)}
  /* Arrastar e soltar */
  #projectsContent{position:relative}
  .proj-drop{position:absolute; inset:0; display:none; align-items:center;
    justify-content:center; background:rgba(10,14,22,.92);
    border:2px dashed var(--live); border-radius:16px; z-index:20;
    pointer-events:none}
  .proj-drop.show{display:flex}
  .pd-inner{text-align:center; color:var(--live); font-size:1.4rem; font-weight:600;
    line-height:1.6}
  .pd-inner{font-size:2.5rem}
  /* Arquivo clicável (abrir no fatiador) */
  .proj-openable{cursor:pointer}
  /* Reorganizar arrastando */
  .proj-item.dragging{opacity:.4}
  .proj-folder.drop-hover{border-color:var(--live); background:rgba(0,122,204,.18);
    box-shadow:0 0 0 2px var(--live) inset}
  .crumb.drop-hover{background:rgba(0,122,204,.25); color:var(--live)}
  .proj-item[draggable="true"]{cursor:grab}
  .proj-item.proj-openable[draggable="true"]{cursor:pointer}
  .proj-openable:hover{border-color:var(--live); background:#111826}
  .proj-openable:hover .pi-ext{transform:scale(1.05); transition:transform .15s}
  /* Modal abrir no fatiador */
  .abrir-body{padding:1.2rem 1.4rem}
  .abrir-file{font-family:'JetBrains Mono',monospace; font-size:.85rem; color:var(--ink);
    background:#0a0e16; border:1px solid var(--hair); border-radius:8px;
    padding:.6rem .8rem; margin-bottom:1rem; word-break:break-all}
  .abrir-q{font-size:.92rem; color:var(--muted); margin:0 0 1rem}
  .abrir-imp{display:flex; align-items:center; gap:.9rem; width:100%;
    background:#0c1119; border:1px solid var(--hair); border-radius:12px;
    padding:.85rem 1rem; margin-bottom:.6rem; cursor:pointer; font-family:inherit;
    color:var(--ink); text-align:left; transition:all .15s}
  .abrir-imp:hover{border-color:var(--live); background:#111826}
  .ai-ic{font-size:1.6rem; flex:0 0 auto}
  .ai-tx{flex:1; min-width:0}
  .ai-tx b{display:block; font-size:.95rem}
  .ai-tx small{color:var(--muted); font-size:.76rem}
  .ai-arrow{font-size:1.4rem; color:var(--faint)}
  .abrir-hint{font-size:.74rem; color:var(--faint); margin-top:.8rem; line-height:1.5}
  .abrir-input{width:100%; background:#0a0e16; border:1px solid var(--hair);
    border-radius:8px; padding:.6rem .8rem; color:var(--ink); font-family:'JetBrains Mono',monospace;
    font-size:.8rem; margin-bottom:.3rem}
  .abrir-input:focus{outline:none; border-color:var(--live)}
  /* Modais (relatórios/calculadora) embutidos como página, sem cara de card flutuante */
  .modal.embedded{background:transparent; border:0; box-shadow:none;
    max-height:none; border-radius:0}
  .modal.embedded .rep-head,.modal.embedded .calc-head{padding-left:0; padding-right:0;
    padding-top:0; border-bottom:1px solid var(--hair)}
  .modal.embedded .rep-body,.modal.embedded .calc-body{padding-left:0; padding-right:0;
    max-height:none; overflow:visible}
  .dash-empty{text-align:center; padding:2.5rem 1rem; color:var(--faint)}
  .dash-empty b{display:block; color:var(--muted); margin-bottom:.4rem; font-size:1rem}
  .mini-donut{display:flex; align-items:center; gap:1.2rem}
  .mini-donut svg{flex:0 0 auto}
  .md-legend{display:flex; flex-direction:column; gap:.5rem; font-size:.82rem}
  .md-legend div{display:flex; align-items:center; gap:.5rem}
  .md-legend i{width:10px; height:10px; border-radius:2px}
  .settings-grid{display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:1rem}
  .set-card{background:#0c1119; border:1px solid var(--hair); border-radius:14px;
    padding:1.2rem; cursor:pointer; transition:all .15s; text-align:left; font-family:inherit;
    color:var(--ink); display:flex; align-items:center; gap:1rem}
  .set-card:hover{border-color:var(--live); background:#111826}
  .set-ic{font-size:1.6rem; flex:0 0 auto}
  .set-tx b{display:block; font-size:.95rem; margin-bottom:.2rem}
  .set-tx small{color:var(--muted); font-size:.78rem}

  /* ── Grade de cards ───────────────────────────────────── */
  main{
    display:grid; gap:1.25rem; padding:clamp(1rem,4vw,2.4rem);
    grid-template-columns:repeat(auto-fill,minmax(370px,1fr));
  }
  @media (max-width:520px){
    main{grid-template-columns:1fr; padding:1rem}
  }
  /* Colunas fixas escolhidas pelo usuário (sobrepõe qualquer modo) */
  main.cols-fixed{
    grid-template-columns:repeat(var(--cols-override,3),minmax(0,1fr)) !important;
  }

  .card{
    position:relative; display:flex; gap:1.1rem; padding:1.1rem 1.1rem 0;
    border:1px solid var(--hair); border-radius:16px; overflow:hidden;
    background:
      linear-gradient(180deg, rgba(255,255,255,.018), transparent 40%),
      linear-gradient(180deg, var(--panel), var(--panel-2));
    box-shadow:0 1px 0 rgba(255,255,255,.03) inset, 0 14px 34px -22px #000;
    flex-direction:column;
  }
  .card::before{ /* fio de luz no topo = metal escovado */
    content:""; position:absolute; top:0; left:0; right:0; height:1px;
    background:linear-gradient(90deg,transparent,var(--hair-lit),transparent); opacity:.7}
  .card .accent{position:absolute; left:0; top:0; bottom:0; width:3px;
    background:var(--ac,#48526a)}
  .card.s-printing{box-shadow:0 1px 0 rgba(255,255,255,.03) inset,
    0 14px 34px -22px #000, 0 0 0 1px rgba(79,140,255,.12), 0 0 34px -14px var(--c-glow)}
  .card.offline{opacity:.5}

  .card.s-printing{--ac:#4f8cff;--c-lit:#5b9bff;--c-deep:#2747a8;--c-glow:rgba(79,140,255,.55);--scan:#aecbff}
  .card.s-paused{--ac:#ffcc44;--c-lit:#ffd45e;--c-deep:#b9892a;--c-glow:rgba(255,204,68,.4);--scan:#ffe49a}
  .card.s-finish{--ac:#37d399;--c-lit:#46e0a8;--c-deep:#1f8e68;--c-glow:rgba(55,211,153,.4);--scan:#9bf0d2}
  .card.s-failed{--ac:#ff5470;--c-lit:#ff6b84;--c-deep:#aa2b41;--c-glow:rgba(255,84,112,.45);--scan:#ffb0bf}
  .card.s-idle{--ac:#5b6b86;--c-lit:#3a465c;--c-deep:#222b3c;--c-glow:transparent;--scan:transparent}

  .row1{display:flex; gap:1.1rem}

  /* câmara de construção = assinatura */
  .chamber{
    position:relative; width:96px; flex:0 0 96px; height:188px;
    border-radius:11px; overflow:hidden; border:1px solid var(--hair);
    background:linear-gradient(180deg,#0a0e16,#0c1119);
    box-shadow:inset 0 1px 0 rgba(255,255,255,.05), inset 0 0 30px rgba(0,0,0,.5);
  }
  .chamber .plate{position:absolute; inset:0;
    background-image:
      linear-gradient(rgba(79,140,255,.10) 1px,transparent 1px),
      linear-gradient(90deg,rgba(79,140,255,.10) 1px,transparent 1px);
    background-size:12px 12px; opacity:.45}
  .chamber .fill{position:absolute; left:0; right:0; bottom:0;
    background:linear-gradient(0deg,var(--c-deep),var(--c-lit));
    transition:height .7s cubic-bezier(.4,0,.2,1);
    box-shadow:0 0 26px 1px var(--c-glow)}
  .chamber .fill::after{content:""; position:absolute; inset:0;
    background:repeating-linear-gradient(0deg,rgba(255,255,255,.12) 0 3px,transparent 3px 6px);
    mix-blend-mode:overlay; opacity:.5}
  .chamber .scan{position:absolute; left:-12%; right:-12%; height:2px; display:none;
    background:linear-gradient(90deg,transparent,var(--scan),transparent);
    box-shadow:0 0 12px 1px var(--scan); transition:bottom .7s cubic-bezier(.4,0,.2,1);
    animation:scan 2.6s ease-in-out infinite}
  .card.s-printing .chamber .scan{display:block}  @keyframes scan{0%,100%{opacity:.55;transform:translateY(2px)}50%{opacity:1;transform:translateY(-2px)}}
  .chamber .read{position:absolute; inset:0; display:flex; flex-direction:column;
    align-items:center; justify-content:center; gap:3px; text-align:center}
  .chamber .pc{font-family:'JetBrains Mono',monospace; font-weight:700;
    font-size:1.3rem; color:#fff; mix-blend-mode:difference; letter-spacing:.01em}
  .chamber .ly{font-family:'JetBrains Mono',monospace; font-size:.6rem;
    color:rgba(255,255,255,.85); mix-blend-mode:difference; letter-spacing:.05em}

  .body{flex:1; min-width:0; display:flex; flex-direction:column; gap:.7rem}
  .top{display:flex; align-items:flex-start; justify-content:space-between; gap:.6rem}
  .pid{min-width:0}
  .pname{font-size:1.18rem; font-weight:600; letter-spacing:.01em; line-height:1.1}
  .tags{display:flex; gap:.4rem; margin-top:.35rem; flex-wrap:wrap}
  .tag{font-family:'JetBrains Mono',monospace; font-size:.6rem; color:var(--muted);
    border:1px solid var(--hair); border-radius:5px; padding:.12rem .4rem;
    display:flex; align-items:center; gap:.3rem}
  .tag-ip{color:var(--live); border-color:rgba(79,140,255,.35);
    background:rgba(79,140,255,.06)}
  .pill{font-family:'JetBrains Mono',monospace; font-size:.64rem; font-weight:700;
    text-transform:uppercase; letter-spacing:.09em; padding:.32rem .6rem;
    border-radius:999px; border:1px solid var(--ac); color:var(--ac);
    white-space:nowrap; background:color-mix(in srgb,var(--ac) 10%, transparent)}

  .job .stage{font-family:'JetBrains Mono',monospace; font-size:.62rem;
    letter-spacing:.12em; text-transform:uppercase; color:var(--faint); margin-bottom:.2rem}
  .job .obj{font-size:.96rem; line-height:1.25; overflow:hidden;
    text-overflow:ellipsis; white-space:nowrap}
  .pbar{height:5px; border-radius:99px; background:#151d2b; overflow:hidden;
    margin-top:.5rem; border:1px solid var(--hair)}
  .pbar i{display:block; height:100%; border-radius:99px;
    background:linear-gradient(90deg,var(--c-deep),var(--c-lit));
    transition:width .7s cubic-bezier(.4,0,.2,1); position:relative}
  .card.s-printing .pbar i::after{content:""; position:absolute; inset:0;
    background:linear-gradient(90deg,transparent,rgba(255,255,255,.5),transparent);
    transform:translateX(-100%); animation:sheen 2.4s ease-in-out infinite}
  @keyframes sheen{0%{transform:translateX(-100%)}60%,100%{transform:translateX(220%)}}

  .metrics{display:grid; grid-template-columns:repeat(4,1fr); gap:.5rem .4rem}
  .m{display:flex; flex-direction:column; gap:.12rem}
  .m .k{font-family:'JetBrains Mono',monospace; font-size:.56rem;
    letter-spacing:.1em; text-transform:uppercase; color:var(--faint)}
  .m .v{font-family:'JetBrains Mono',monospace; font-size:.92rem; color:var(--ink)}
  .m .v small{color:var(--muted); font-size:.7rem}

  .env{display:flex; gap:.5rem; flex-wrap:wrap; align-items:center}
  .chip{display:flex; align-items:center; gap:.34rem; padding:.28rem .5rem;
    border:1px solid var(--hair); border-radius:7px; background:#0c121c;
    font-family:'JetBrains Mono',monospace; font-size:.72rem}
  .chip .cl{font-size:.55rem; color:var(--faint); letter-spacing:.08em}
  .chip.hot .vv{color:var(--heat); text-shadow:0 0 9px rgba(255,122,61,.4)}
  .chip .vv small{color:var(--muted); font-size:.66rem}
  .wifi{display:flex; align-items:flex-end; gap:1.5px; height:11px}
  .wifi i{width:2.5px; background:var(--faint); border-radius:1px}
  .wifi i.on{background:var(--live)}

  .ams{display:flex; gap:.5rem; flex-wrap:wrap; align-items:center;
    padding:.6rem 0 0; border-top:1px solid var(--hair); margin-top:.1rem}
  .ams .lbl{font-family:'JetBrains Mono',monospace; font-size:.56rem;
    letter-spacing:.14em; text-transform:uppercase; color:var(--faint)}
  .slot{display:flex; align-items:center; gap:.4rem; padding:.22rem .45rem;
    border:1px solid var(--hair); border-radius:8px; background:#0c121c;
    font-family:'JetBrains Mono',monospace; font-size:.7rem}
  .slot .sw{width:14px;height:14px;border-radius:4px;border:1px solid #ffffff2e;
    flex:0 0 14px; background-clip:padding-box}
  .slot.active{border-color:var(--ac);
    box-shadow:0 0 0 1px var(--ac), 0 0 12px -3px var(--c-glow)}
  .slot .rm{color:var(--muted)}

  .alert{display:flex; align-items:center; gap:.5rem; flex-wrap:wrap;
    margin:.2rem -1.1rem 0; padding:.55rem 1.1rem;
    background:linear-gradient(90deg,rgba(255,84,112,.14),transparent);
    border-top:1px solid rgba(255,84,112,.3);
    font-family:'JetBrains Mono',monospace; font-size:.72rem; color:var(--fail)}
  .alert a{color:var(--warn)}

  .foot{min-height:.9rem; display:flex; justify-content:center; margin-top:.4rem}
  .empty{grid-column:1/-1; text-align:center; color:var(--muted);
    padding:5rem 1rem; font-family:'JetBrains Mono',monospace; font-size:.85rem}
  .empty b{color:var(--ink); font-weight:500; display:block; margin-bottom:.4rem;
    font-size:1rem}

  @media (prefers-reduced-motion: reduce){
    .chamber .scan,.dot.live,.pbar i::after,.card{animation:none}
  }

  /* ── Botão e modo painel de parede (kiosk) ───────────── */
  .kbtn{font-family:'JetBrains Mono',monospace; font-size:.64rem; letter-spacing:.08em;
    text-transform:uppercase; color:#555; background:#f3f4f6;
    border:1px solid #d1d5db; border-radius:8px; padding:.42rem .7rem; cursor:pointer;
    display:flex; align-items:center; gap:.4rem; transition:.15s;
    white-space:nowrap; flex:0 0 auto}
  .kbtn:hover{color:#111; border-color:#9ca3af}
  .kexit{display:none; position:fixed; top:1rem; right:1rem; z-index:90; cursor:pointer;
    background:#0c121cdd; border:1px solid var(--hair); border-radius:8px; padding:.5rem .8rem;
    font-family:'JetBrains Mono',monospace; font-size:.7rem; color:var(--muted)}
  .kexit:hover{color:var(--ink)}
  body.kiosk .kbtn{display:none}
  body.kiosk .kexit{display:block}
  /* Modo parede: só a farm, sem menu lateral nem barra de topo */
  body.kiosk .sidebar{display:none}
  body.kiosk .topbar{display:none}
  body.kiosk .printers-head{display:none}
  body.kiosk .page{padding:0}
  body.kiosk .content{width:100vw}
  body.kiosk main{gap:1.6rem; padding:1.6rem; grid-template-columns:repeat(auto-fit,minmax(470px,1fr))}
  body.kiosk .card{padding:1.7rem 1.7rem 1.2rem}
  body.kiosk .chamber{width:148px; flex:0 0 148px; height:264px}
  body.kiosk .chamber .pc{font-size:2.5rem}
  body.kiosk .chamber .ly{font-size:.82rem}
  body.kiosk .pname{font-size:2.1rem}
  body.kiosk .pill{font-size:.86rem; padding:.42rem .85rem}
  body.kiosk .tags,body.kiosk .pbar,body.kiosk .env,body.kiosk .ams-wrap{display:none}
  body.kiosk .obj{font-size:1.45rem; white-space:normal}
  body.kiosk .stage{font-size:.86rem}
  body.kiosk .metrics{grid-template-columns:repeat(2,1fr); gap:.6rem 1.4rem; margin-top:.4rem}
  body.kiosk .metrics .m:nth-child(n+3){display:none}
  body.kiosk .metrics .k{font-size:.74rem}
  body.kiosk .metrics .v{font-size:2rem}
  body.kiosk .alert{font-size:1.05rem; padding:.85rem 1.7rem}

  /* ── Custo por impressão ──────────────────────────────── */
  .custo-ask{padding:1.4rem; text-align:center}
  .custo-form{padding:1.2rem 1.4rem}
  .ca-printer{font-size:1.05rem; font-weight:600; color:var(--ink)}
  .ca-file{font-family:'JetBrains Mono',monospace; font-size:.76rem; color:var(--muted);
    margin-top:.2rem; word-break:break-all}
  .ca-q{font-size:1rem; color:var(--ink); margin:1.3rem 0 1.1rem}
  .ca-btns{display:flex; gap:.6rem}
  .ca-btns button{flex:1; cursor:pointer; font-family:inherit; font-size:.88rem;
    font-weight:600; border-radius:10px; padding:.75rem}
  .ca-no{background:#ffffff0d; border:1px solid var(--hair); color:var(--muted)}
  .ca-no:hover{color:var(--ink); border-color:var(--muted)}
  .ca-yes{background:var(--ac); border:0; color:#fff}
  .ca-yes:hover{filter:brightness(1.1)}
  .ca-hint{font-size:.72rem; color:var(--faint); margin-top:.9rem; line-height:1.5}
  .cf-row{display:flex; align-items:center; gap:.6rem; margin-bottom:.6rem}
  .cf-row label{flex:1; font-size:.85rem; color:var(--ink)}
  .cf-row input,.cf-row select{width:120px; background:#0a0e16;
    border:1px solid var(--hair); border-radius:8px; padding:.5rem .6rem;
    color:var(--ink); font-family:'JetBrains Mono',monospace; font-size:.86rem;
    text-align:right}
  .cf-row select{text-align:left; font-family:inherit}
  .cf-row input:focus,.cf-row select:focus{outline:none; border-color:var(--live)}
  .cf-u{font-size:.7rem; color:var(--faint); width:44px}
  .cf-out{margin-top:1rem; background:linear-gradient(180deg,#0d1420,#0a0e16);
    border:1px solid var(--hair); border-radius:12px; padding:.9rem 1rem;
    font-size:.84rem; color:var(--muted)}
  .cf-l{display:flex; justify-content:space-between; padding:.3rem 0}
  .cf-l b{font-family:'JetBrains Mono',monospace; color:var(--ink)}
  .cf-t{display:flex; justify-content:space-between; align-items:baseline;
    margin-top:.5rem; padding-top:.6rem; border-top:2px solid var(--hair)}
  .cf-t span{font-size:.76rem; text-transform:uppercase; letter-spacing:.06em}
  .cf-t b{font-family:'JetBrains Mono',monospace; font-size:1.35rem; color:#35d17c}
  /* selo de custo no card */
  .cost-chip{font-family:'JetBrains Mono',monospace; font-size:.66rem; font-weight:700;
    color:#35d17c; background:rgba(53,209,124,.1);
    border:1px solid rgba(53,209,124,.35); border-radius:5px;
    padding:.16rem .45rem; display:inline-flex; align-items:center; gap:.25rem}
  .gram-chip{font-family:'JetBrains Mono',monospace; font-size:.66rem; font-weight:700;
    color:#e0a94f; background:rgba(224,169,79,.1);
    border:1px solid rgba(224,169,79,.35); border-radius:5px;
    padding:.16rem .45rem; display:inline-flex; align-items:center; gap:.25rem}

  /* ── Calculadora de custo ─────────────────────────────── */
  .calc-head{display:flex; align-items:center; justify-content:space-between;
    padding:1.1rem 1.4rem; border-bottom:1px solid var(--hair)}
  .calc-head b{font-size:1.1rem}
  .calc-body{padding:1.2rem 1.4rem; max-height:76vh; overflow:auto}
  .calc-grid{display:grid; grid-template-columns:1fr 1fr; gap:1.2rem}
  .calc-sec{background:#ffffff06; border:1px solid var(--hair);
    border-radius:12px; padding:.9rem 1rem; margin-bottom:.9rem}
  .calc-sec h4{font-size:.72rem; text-transform:uppercase; letter-spacing:.1em;
    color:var(--muted); margin:0 0 .7rem; font-weight:600}
  .calc-row{display:flex; align-items:center; gap:.6rem; margin-bottom:.55rem}
  .calc-row label{flex:1; font-size:.82rem; color:var(--ink)}
  .calc-row .unit{font-size:.7rem; color:var(--faint); width:42px; text-align:right}
  .calc-row input{width:110px; background:#0a0e16; border:1px solid var(--hair);
    border-radius:8px; padding:.45rem .6rem; color:var(--ink);
    font-family:'JetBrains Mono',monospace; font-size:.85rem; text-align:right}
  .calc-row input:focus{outline:none; border-color:var(--live)}
  .calc-pull{width:100%; margin-bottom:.9rem; cursor:pointer; font-family:inherit;
    background:rgba(79,140,255,.1); border:1px solid rgba(79,140,255,.35);
    color:var(--live); border-radius:10px; padding:.6rem; font-size:.82rem; font-weight:600}
  .calc-pull:hover{background:rgba(79,140,255,.2)}
  .calc-out{background:linear-gradient(180deg,#0d1420,#0a0e16);
    border:1px solid var(--hair); border-radius:12px; padding:1rem}
  .calc-line{display:flex; justify-content:space-between; align-items:center;
    padding:.42rem 0; font-size:.85rem; border-bottom:1px dashed var(--hair)}
  .calc-line:last-child{border-bottom:0}
  .calc-line span:first-child{color:var(--muted)}
  .calc-line b{font-family:'JetBrains Mono',monospace; color:var(--ink)}
  .calc-total{display:flex; justify-content:space-between; align-items:baseline;
    margin-top:.7rem; padding-top:.7rem; border-top:2px solid var(--hair)}
  .calc-total span{font-size:.78rem; color:var(--muted); text-transform:uppercase;
    letter-spacing:.08em}
  .calc-total b{font-family:'JetBrains Mono',monospace; font-size:1.5rem; color:var(--live)}
  .calc-sell{background:rgba(53,209,124,.08); border:1px solid rgba(53,209,124,.35);
    border-radius:12px; padding:.9rem 1rem; margin-top:.8rem;
    display:flex; justify-content:space-between; align-items:baseline}
  .calc-sell span{font-size:.78rem; color:#8fe3b4; text-transform:uppercase;
    letter-spacing:.08em}
  .calc-sell b{font-family:'JetBrains Mono',monospace; font-size:1.7rem; color:#35d17c}
  .calc-note{font-size:.72rem; color:var(--faint); margin-top:.7rem; line-height:1.5}
  .calc-save{margin-top:.8rem; width:100%; cursor:pointer; font-family:inherit;
    background:var(--ac); border:0; color:#fff; border-radius:10px;
    padding:.65rem; font-size:.85rem; font-weight:600}
  .calc-save:hover{filter:brightness(1.1)}
  @media(max-width:820px){ .calc-grid{grid-template-columns:1fr} }

  /* ── Menu principal ───────────────────────────────────── */
  .menu-head{display:flex; align-items:center; justify-content:space-between;
    padding:1.1rem 1.4rem; border-bottom:1px solid var(--hair)}
  .menu-head b{font-size:1.1rem}
  .menu-body{padding:.8rem}
  .menu-item{display:flex; align-items:center; gap:.9rem; width:100%;
    background:#ffffff06; border:1px solid var(--hair); border-radius:12px;
    padding:.8rem 1rem; margin-bottom:.5rem; cursor:pointer; text-align:left;
    font-family:inherit; color:var(--ink); transition:all .15s}
  .menu-item:hover{background:#ffffff12; border-color:var(--ac)}
  .menu-item.primary{background:var(--ac); border-color:var(--ac); color:#fff}
  .menu-item.primary:hover{filter:brightness(1.08)}
  .menu-item.primary .mi-tx small{color:#ffffffcc}
  .mi-ic{font-size:1.3rem; flex:0 0 auto; width:28px; text-align:center}
  .mi-tx{flex:1; display:flex; flex-direction:column; gap:.1rem; min-width:0}
  .mi-tx b{font-size:.92rem; font-weight:600}
  .mi-tx small{font-size:.72rem; color:var(--muted)}
  .mi-val{flex:0 0 auto; font-size:.74rem; font-weight:600; color:var(--ac);
    background:#ffffff10; border-radius:20px; padding:.3rem .7rem; white-space:nowrap}
  .menu-item.primary .mi-val{color:#fff; background:#ffffff2a}
  .menu-sep{font-size:.68rem; text-transform:uppercase; letter-spacing:.1em;
    color:var(--muted); padding:.8rem 1rem .4rem; font-weight:600}

  /* ── Relatórios ───────────────────────────────────────── */
  .rep-head{display:flex; align-items:center; justify-content:space-between;
    padding:1.1rem 1.4rem; border-bottom:1px solid var(--hair)}
  .rep-head b{font-size:1.1rem}
  .rep-loading{padding:3rem; text-align:center; color:var(--muted)}
  .rep-body{padding:1.2rem 1.4rem; max-height:80vh; overflow-y:auto}
  .rep-controls{display:flex; align-items:center; justify-content:space-between; gap:1rem; margin-bottom:1rem; flex-wrap:wrap}
  .rep-periods{display:flex; gap:.4rem}
  .rep-pbtn{cursor:pointer; font-family:inherit; font-size:.78rem; font-weight:600;
    color:var(--muted); background:#ffffff08; border:1px solid var(--hair);
    border-radius:8px; padding:.4rem .9rem; transition:all .15s}
  .rep-pbtn:hover{color:var(--ink); border-color:var(--ac)}
  .rep-pbtn.on{color:#fff; background:var(--ac); border-color:var(--ac)}
  .rep-pdf{cursor:pointer; font-family:inherit; font-size:.78rem; font-weight:700;
    color:#fff; background:#37d67a; border:0; border-radius:8px; padding:.45rem 1rem;
    text-decoration:none; transition:all .15s}
  .rep-pdf:hover{background:#2fc06c}
  .rep-filter{display:flex; align-items:center; gap:.4rem; flex-wrap:wrap; margin-bottom:1rem}
  .rep-flabel{font-size:.72rem; color:var(--muted); margin-right:.3rem}
  .rep-chip{cursor:pointer; font-family:inherit; font-size:.72rem; color:var(--muted);
    background:#ffffff08; border:1px solid var(--hair); border-radius:20px; padding:.28rem .7rem}
  .rep-chip.on{color:#fff; background:var(--ac); border-color:var(--ac)}
  .rep-chip-clear{cursor:pointer; font-family:inherit; font-size:.72rem; color:var(--ac);
    background:none; border:0; text-decoration:underline}
  .rep-cards{display:grid; grid-template-columns:repeat(auto-fit,minmax(115px,1fr)); gap:.8rem; margin-bottom:1.2rem}
  .rep-stat{background:#ffffff06; border:1px solid var(--hair); border-radius:12px;
    padding:1rem; text-align:center}
  .rep-stat-v{font-size:1.9rem; font-weight:800; font-family:'JetBrains Mono',monospace}
  .rep-stat-k{font-size:.72rem; color:var(--muted); margin-top:.2rem}
  .rep-charts{display:grid; grid-template-columns:200px 1fr; gap:1rem; margin-bottom:1.4rem}
  @media(max-width:640px){.rep-charts{grid-template-columns:1fr}.rep-cards{grid-template-columns:repeat(2,1fr)}}
  .rep-chart-box{background:#ffffff06; border:1px solid var(--hair); border-radius:12px; padding:1rem}
  .rep-chart-title{font-size:.78rem; color:var(--muted); margin-bottom:.8rem; font-weight:600}
  .rep-donut{width:130px; height:130px; display:block; margin:0 auto}
  .rep-donut-pct{fill:var(--ink); font-size:24px; font-weight:800; font-family:'JetBrains Mono',monospace}
  .rep-donut-lbl{fill:var(--muted); font-size:11px}
  .rep-bars{display:flex; flex-direction:column; gap:.5rem}
  .rep-bar-row{display:flex; align-items:center; gap:.7rem}
  .rep-bar-name{width:120px; font-size:.74rem; white-space:nowrap; overflow:hidden;
    text-overflow:ellipsis; flex:0 0 auto}
  .rep-bar-track{flex:1; height:16px; background:#ffffff08; border-radius:6px; overflow:hidden}
  .rep-bar-fill{height:100%; background:#4f8cff; border-radius:6px; position:relative; min-width:2px}
  .rep-bar-succ{position:absolute; left:0; top:0; bottom:0; background:#37d67a; border-radius:6px}
  .rep-bar-val{width:32px; text-align:right; font-size:.78rem; font-weight:700;
    font-family:'JetBrains Mono',monospace; flex:0 0 auto}
  .rep-empty{color:var(--muted); font-size:.8rem; text-align:center; padding:1.5rem}
  .rep-table-title{font-size:.85rem; font-weight:700; margin-bottom:.6rem}
  .rep-table-wrap{max-height:300px; overflow-y:auto; border:1px solid var(--hair); border-radius:10px}
  .rep-table{width:100%; border-collapse:collapse; font-size:.76rem}
  .rep-table th{position:sticky; top:0; background:#1a1f2e; color:#fff; text-align:left;
    padding:.5rem .7rem; font-weight:600}
  .rep-table td{padding:.45rem .7rem; border-top:1px solid var(--hair)}
  .rep-file{max-width:200px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis}
  .rep-res{font-size:.7rem; font-weight:700; padding:.15rem .5rem; border-radius:6px}
  .rep-res.ok{color:#37d67a; background:#37d67a1a}
  .rep-res.fail{color:#ff5470; background:#ff54701a}

  /* ── Modos de visualização dos cards ──────────────────── */
  /* 2. COMPACTO: cards menores, cabe mais por linha */
  main.view-compact{grid-template-columns:repeat(auto-fill,minmax(270px,1fr)); gap:.8rem}
  main.view-compact .chamber{width:56px; flex:0 0 56px; height:110px}
  main.view-compact .chamber .pc{font-size:.9rem}
  main.view-compact .chamber .ly{display:none}
  main.view-compact .pname{font-size:.95rem}
  main.view-compact .tags,main.view-compact .env,main.view-compact .ams-wrap,
  main.view-compact .stage,main.view-compact .foot,main.view-compact .obj{display:none}
  main.view-compact .metrics{grid-template-columns:repeat(2,1fr)}
  main.view-compact .metrics .m:nth-child(n+3){display:none}
  main.view-compact .card{padding:.8rem .8rem 0}

  /* 3. LISTA: uma linha por impressora */
  main.view-list{grid-template-columns:1fr; gap:.5rem}
  main.view-list .card{flex-direction:row; align-items:center; gap:1rem; padding:.55rem 1rem}
  main.view-list .row1{flex:1; flex-direction:row; align-items:center; gap:1rem}
  main.view-list .chamber{width:34px; flex:0 0 34px; height:34px; border-radius:8px}
  main.view-list .chamber .read,main.view-list .chamber .scan{display:none}
  main.view-list .body{flex:1; display:flex; flex-direction:row; align-items:center; gap:1.2rem}
  main.view-list .top{flex:0 0 auto; min-width:150px}
  main.view-list .pname{font-size:.92rem}
  main.view-list .tags,main.view-list .env,main.view-list .ams-wrap,
  main.view-list .foot,main.view-list .obj,main.view-list .stage{display:none}
  main.view-list .job{flex:1; min-width:120px}
  main.view-list .metrics{flex:0 0 auto; grid-template-columns:repeat(4,auto); gap:.2rem 1.1rem; margin:0}
  main.view-list .metrics .m{flex-direction:row; gap:.35rem; align-items:baseline}
  main.view-list .metrics .v{font-size:.88rem}
  main.view-list .metrics .k{font-size:.6rem}
  main.view-list .metrics .m:nth-child(n+3){display:none}

  /* 4. FOCO: progresso em destaque, resto minimizado */
  main.view-focus{grid-template-columns:repeat(auto-fill,minmax(320px,1fr))}
  main.view-focus .chamber,main.view-focus .tags,main.view-focus .env,
  main.view-focus .ams-wrap,main.view-focus .foot,
  main.view-focus .metrics .m:nth-child(n+3){display:none}
  main.view-focus .pname{font-size:1.15rem}
  main.view-focus .obj{font-size:.95rem}
  main.view-focus .pbar{height:14px; border-radius:8px}
  main.view-focus .metrics{grid-template-columns:repeat(2,1fr); gap:.5rem}
  main.view-focus .metrics .v{font-size:1.9rem}
  main.view-focus .metrics .k{font-size:.72rem}

  /* 5. MOSAICO: quadradinhos mínimos = mapa da farm */
  main.view-mosaic{grid-template-columns:repeat(auto-fill,minmax(150px,1fr)); gap:.6rem}
  main.view-mosaic .card{padding:0; aspect-ratio:1}
  main.view-mosaic .chamber,main.view-mosaic .tags,main.view-mosaic .env,
  main.view-mosaic .ams-wrap,main.view-mosaic .metrics,main.view-mosaic .stage,
  main.view-mosaic .foot,main.view-mosaic .obj,main.view-mosaic .cardtools{display:none}
  main.view-mosaic .card::after{content:""; position:absolute; inset:0;
    background:var(--ac,#3a4150); opacity:.16; pointer-events:none}
  main.view-mosaic .row1{position:absolute; inset:0; flex-direction:column;
    align-items:center; justify-content:center; padding:.5rem}
  main.view-mosaic .body{align-items:center; justify-content:center}
  main.view-mosaic .top{flex-direction:column; align-items:center; gap:.4rem}
  main.view-mosaic .pname{font-size:1rem; text-align:center; z-index:2}
  main.view-mosaic .job{position:absolute; left:0; right:0; bottom:0}
  main.view-mosaic .pbar{height:8px; border-radius:0}
  main.view-mosaic .pill{z-index:2}

  /* ── Modal de detalhe ─────────────────────────────────── */
  .card{cursor:pointer}
  .overlay{position:fixed; inset:0; z-index:9999; display:none; padding:1.2rem;
    background:rgba(4,7,12,.74); backdrop-filter:blur(6px);
    align-items:center; justify-content:center}
  .overlay.open{display:flex}
  .modal{width:min(960px,96vw); max-height:92vh; overflow:auto; position:relative;
    background:linear-gradient(180deg,var(--panel),var(--panel-2));
    border:1px solid var(--hair); border-radius:18px; box-shadow:0 34px 90px -34px #000}
  .mhead{display:flex; align-items:flex-start; justify-content:space-between; gap:1rem;
    padding:1.2rem 1.4rem; border-bottom:1px solid var(--hair)}
  .mh-name{font-size:1.4rem; font-weight:600; display:flex; align-items:center; gap:.7rem}
  .mh-obj{color:var(--muted); font-size:.9rem; margin-top:.25rem; overflow:hidden;
    text-overflow:ellipsis; white-space:nowrap; max-width:60vw}
  .mclose{cursor:pointer; border:1px solid var(--hair); border-radius:8px; background:#0c121c;
    color:var(--muted); padding:.3rem .6rem; font-size:1rem; line-height:1.2}
  .mclose:hover{color:var(--ink)}
  .mgrid{display:grid; grid-template-columns:1.25fr 1fr; gap:1.1rem; padding:1.2rem 1.4rem .4rem}
  @media(max-width:760px){.mgrid{grid-template-columns:1fr}}
  .panel{border:1px solid var(--hair); border-radius:12px; background:#0b1019; overflow:hidden}
  .ph{font-family:'JetBrains Mono',monospace; font-size:.58rem; letter-spacing:.16em;
    text-transform:uppercase; color:var(--faint); padding:.7rem .9rem .45rem}
  .cam{aspect-ratio:16/9; display:flex; align-items:center; justify-content:center; text-align:center;
    color:var(--muted); font-size:.8rem; line-height:1.5; padding:1rem; position:relative; overflow:hidden;
    background:repeating-linear-gradient(45deg,#0a0e16 0 13px,#0c1119 13px 26px)}
  .cam img,.cam video{width:100%; height:100%; object-fit:contain; background:#000}
  .cam .cam-off{color:var(--muted); font-size:.75rem; line-height:1.5}
  .cam .cam-load{position:absolute; color:var(--muted); font-size:.7rem;
    font-family:'JetBrains Mono',monospace}
  .modal-cam-btn{float:right; cursor:pointer; font-family:inherit; font-size:.68rem;
    font-weight:600; color:var(--muted); background:#ffffff0a; border:1px solid var(--hair);
    border-radius:7px; padding:.24rem .6rem; transition:all .15s}
  .modal-cam-btn:hover{color:var(--ink); background:#ffffff16; border-color:var(--ac)}
  .modal-cam-btn.on{color:#fff; background:var(--ac); border-color:var(--ac)}
  .legend{display:flex; gap:1.1rem; padding:0 .9rem .5rem; font-family:'JetBrains Mono',monospace;
    font-size:.66rem; color:var(--muted)}
  .legend i{display:inline-block; width:11px; height:3px; border-radius:2px; margin-right:.35rem;
    vertical-align:middle}
  .chart{padding:.2rem .5rem .3rem}
  .chart .coll{color:var(--faint); font-size:.76rem; font-family:'JetBrains Mono',monospace;
    padding:1.6rem .9rem; text-align:center}
  .bigtemps{display:flex; gap:1.4rem; padding:.4rem .95rem .9rem; font-family:'JetBrains Mono',monospace}
  .bigtemps span{font-size:.56rem; color:var(--faint); display:block; letter-spacing:.12em}
  .bigtemps b{font-size:1.4rem; font-weight:500}
  .mdetail{display:grid; grid-template-columns:repeat(3,1fr); gap:.8rem 1rem; padding:.4rem 1.4rem 1.2rem}
  .alog{max-height:180px; overflow:auto; padding:.3rem .3rem .6rem; margin:0 1.4rem 1.4rem;
    border:1px solid var(--hair); border-radius:12px; background:#0b1019}
  .alog .aph{padding:.7rem .9rem .4rem}
  .ev{display:flex; gap:.7rem; align-items:baseline; padding:.34rem .7rem;
    font-family:'JetBrains Mono',monospace; font-size:.72rem; border-top:1px solid #141b27}
  .ev time{color:var(--faint); flex:0 0 auto}
  .ev .code{color:var(--fail); word-break:break-all}
  .none{color:var(--faint); padding:.5rem .9rem .8rem; font-family:'JetBrains Mono',monospace; font-size:.74rem}

  /* ── Botão destacado, ferramentas do card, arraste ────── */
  .kbtn-primary{color:#fff; background:#00AFF0; border-color:#00AFF0; font-weight:700}
  .kbtn-primary:hover{filter:brightness(1.08); color:#fff}
  .cardtools{position:absolute; top:.55rem; right:.6rem; z-index:5; display:flex; gap:.3rem;
    opacity:0; transition:opacity .15s}
  .card:hover .cardtools{opacity:1}
  .cardtools .grip{cursor:grab; color:var(--faint); font-size:1rem; line-height:1;
    padding:.15rem .3rem; user-select:none}
  .cardtools .grip:active{cursor:grabbing}
  .cardtools .del{cursor:pointer; border:1px solid var(--hair); background:#0c121c;
    color:var(--muted); border-radius:6px; padding:.05rem .35rem; font-size:.8rem; line-height:1.3}
  .cardtools .del:hover{color:var(--fail); border-color:var(--fail)}
  .cardtools .ren{cursor:pointer; border:1px solid var(--hair); background:#0c121c;
    color:var(--faint); border-radius:6px; width:22px; height:22px; font-size:.72rem;
    display:flex; align-items:center; justify-content:center; padding:0}
  .cardtools .ren:hover{color:var(--live); border-color:var(--live)}
  .card.dragging{opacity:.5; outline:1px dashed var(--live)}

  /* ── Assistente de adicionar impressora ───────────────── */
  #addModal{padding:0}
  .wiz-head{display:flex; align-items:center; justify-content:space-between;
    padding:1.1rem 1.3rem; border-bottom:1px solid var(--hair)}
  .wiz-head b{font-size:1.1rem; font-weight:600}
  .wiz-body{padding:1.3rem}
  .wiz-body label{display:block; font-family:'JetBrains Mono',monospace; font-size:.6rem;
    letter-spacing:.12em; text-transform:uppercase; color:var(--faint); margin:.9rem 0 .35rem}
  .wiz-body input, .wiz-body select{width:100%; background:#0a0e16; border:1px solid var(--hair);
    border-radius:9px; padding:.65rem .75rem; color:var(--ink);
    font-family:'JetBrains Mono',monospace; font-size:.86rem}
  .wiz-body input:focus{outline:none; border-color:var(--live)}
  .dev-apelido{width:100%; margin:-.2rem 0 .6rem; background:#0a0e16;
    border:1px solid var(--live); border-radius:8px; padding:.5rem .7rem;
    color:var(--ink); font-family:inherit; font-size:.82rem}
  .brands{display:grid; grid-template-columns:1fr 1fr; gap:.7rem}
  .brand-card{border:1px solid var(--hair); border-radius:12px; padding:1.1rem; cursor:pointer;
    text-align:center; background:#0b1019; transition:.15s}
  .brand-card:hover{border-color:var(--live)}
  .brand-card.soon{opacity:.55; cursor:not-allowed}
  .brand-card .bn{font-weight:600; font-size:1rem; margin-bottom:.2rem}
  .brand-card .bs{font-family:'JetBrains Mono',monospace; font-size:.6rem; color:var(--faint);
    text-transform:uppercase; letter-spacing:.1em}
  .wiz-hint{font-size:.78rem; color:var(--muted); line-height:1.5; margin-top:.3rem}
  .wiz-tabs{display:flex; gap:.5rem; margin-bottom:.4rem}
  .wiz-tab{font-family:'JetBrains Mono',monospace; font-size:.66rem; padding:.35rem .6rem;
    border:1px solid var(--hair); border-radius:7px; cursor:pointer; color:var(--muted)}
  .wiz-tab.on{border-color:var(--live); color:var(--live)}
  .dev-list{display:flex; flex-direction:column; gap:.5rem; max-height:320px; overflow:auto}
  .dev{display:flex; align-items:center; gap:.7rem; padding:.7rem .8rem; border:1px solid var(--hair);
    border-radius:10px; background:#0b1019; cursor:pointer}
  .dev:hover{border-color:var(--hair-lit)}
  .dev.sel{border-color:var(--live); box-shadow:0 0 0 1px var(--live)}
  .dev .dn{font-weight:500}
  .dev .dm{font-family:'JetBrains Mono',monospace; font-size:.66rem; color:var(--faint)}
  .dev .don{margin-left:auto; font-family:'JetBrains Mono',monospace; font-size:.62rem}
  .dev .don.up{color:var(--done)} .dev .don.down{color:var(--faint)}
  .wiz-foot{display:flex; gap:.6rem; justify-content:flex-end; padding:1rem 1.3rem;
    border-top:1px solid var(--hair)}
  .wiz-btn{padding:.6rem 1.1rem; border-radius:9px; border:1px solid var(--hair);
    background:#0c121c; color:var(--ink); cursor:pointer; font-family:'Space Grotesk',sans-serif;
    font-size:.88rem}
  .wiz-btn.primary{background:var(--live); color:#04122e; border-color:var(--live); font-weight:700}
  .wiz-btn:disabled{opacity:.5; cursor:not-allowed}
  .wiz-err{color:var(--fail); font-family:'JetBrains Mono',monospace; font-size:.74rem;
    margin-top:.8rem; min-height:1em}
  .wiz-spin{color:var(--muted); font-family:'JetBrains Mono',monospace; font-size:.8rem;
    text-align:center; padding:1.4rem}
  .auto-detect-box{background:#0b1019; border:1px solid var(--hair); border-radius:12px;
    padding:1rem; margin-bottom:.8rem}
  .auto-detect-label{font-family:'JetBrains Mono',monospace; font-size:.56rem;
    letter-spacing:.16em; text-transform:uppercase; color:var(--faint); margin-bottom:.6rem}
  .auto-detect-btn{width:100%; justify-content:center; font-size:.82rem;
    padding:.65rem; border-color:var(--live); color:var(--live);
    background:rgba(79,140,255,.08)}
  .auto-detect-btn:hover{background:rgba(79,140,255,.16); color:var(--live)}
  .wiz-divider{display:flex; align-items:center; gap:.7rem; margin:.9rem 0 .6rem;
    color:var(--faint); font-size:.72rem; font-family:'JetBrains Mono',monospace}
  .wiz-divider::before,.wiz-divider::after{content:""; flex:1;
    height:1px; background:var(--hair)}

  /* ═══════════════════════════════════════════════════════
     RESPONSIVO — CELULAR E TABLET
     ═══════════════════════════════════════════════════════ */
  @media (max-width:820px){
    /* Header compacto */
    header{padding:.7rem 1rem; gap:.6rem}
    .brand .logo{height:34px}
    .brand .title{font-size:.72rem; letter-spacing:.08em}
    .brand .sep{display:none}
    .kbtn{font-size:.62rem; padding:.45rem .6rem}
    .conn{font-size:.6rem}

    /* Cards: sempre 1 por linha, ignora colunas fixas no celular */
    main{padding:.8rem; gap:.8rem}
    main.cols-fixed{grid-template-columns:1fr !important}
    main:not(.view-list):not(.view-mosaic){grid-template-columns:1fr !important}

    /* Modais ocupam quase a tela toda */
    .overlay{padding:0}
    .modal{width:100% !important; max-width:100% !important;
      max-height:100vh; border-radius:0; min-height:100vh}

    /* Modal de detalhe: painéis empilhados */
    .mgrid{grid-template-columns:1fr; gap:.9rem; padding:1rem}
    .mdetail{grid-template-columns:repeat(2,1fr); padding:.4rem 1rem 1rem}
    .mhead{padding:1rem}
    .cam{aspect-ratio:16/10}

    /* Relatórios em coluna única */
    .rep-charts{grid-template-columns:1fr}
    .rep-cards{grid-template-columns:repeat(2,1fr)}
    .rep-body{padding:1rem; max-height:none}
    .rep-controls{flex-direction:column; align-items:stretch; gap:.7rem}
    .rep-periods{justify-content:space-between}
    .rep-pbtn{flex:1; text-align:center; padding:.5rem .4rem}
    .rep-pdf{text-align:center}
    .rep-table{font-size:.68rem}
    .rep-table th,.rep-table td{padding:.4rem .45rem}
    .rep-table-wrap{max-height:none}
    .rep-bar-name{width:80px}

    /* Menu confortável no dedo */
    .menu-item{padding:.9rem 1rem}
    .mi-ic{font-size:1.4rem}

    /* Login e ativação */
    .box{padding:1.5rem}
    .fp{font-size:.95rem}
  }

  /* Celular estreito */
  @media (max-width:480px){
    .brand .title{display:none}   /* só o logo, economiza espaço */
    .mdetail{grid-template-columns:1fr}
    .rep-cards{grid-template-columns:1fr 1fr}
    .metrics{grid-template-columns:repeat(2,1fr) !important}
    /* Botões do header maiores pro toque */
    .kbtn{padding:.5rem .7rem; font-size:.64rem}
  }

  /* Toque: alvos maiores, sem hover chato */
  @media (hover:none) and (pointer:coarse){
    .kbtn,.menu-item,.rep-pbtn,.rep-chip,.modal-cam-btn,.mclose{
      min-height:40px}
    .card{cursor:default}
  }
</style>
</head>
<body>
<div class="app">
  <!-- ══ SIDEBAR ══ -->
  <aside class="sidebar" id="sidebar">
    <div class="sb-brand">
      <img class="sb-logo" src="__LOGO_SRC__" alt="3DWORK">
    </div>
    <nav class="sb-nav">
      <button class="sb-item active" data-page="dashboard" onclick="nav('dashboard')">
        <span class="sb-ic">▤</span><span class="sb-tx">Dashboard</span></button>
      <button class="sb-item" data-page="printers" onclick="nav('printers')">
        <span class="sb-ic">🖨️</span><span class="sb-tx">Impressoras</span></button>
      <button class="sb-item" data-page="projects" onclick="nav('projects')">
        <span class="sb-ic">📁</span><span class="sb-tx">Projetos</span></button>
      <button class="sb-item" data-page="reports" onclick="nav('reports')">
        <span class="sb-ic">📊</span><span class="sb-tx">Relatórios</span></button>
      <button class="sb-item" data-page="calc" onclick="nav('calc')">
        <span class="sb-ic">🧮</span><span class="sb-tx">Calculadora</span></button>
      <button class="sb-item" data-page="settings" onclick="nav('settings')">
        <span class="sb-ic">⚙</span><span class="sb-tx">Configurações</span></button>
    </nav>
    <div class="sb-foot">
      <button class="sb-item" onclick="abrirWhatsapp('suporte')">
        <span class="sb-ic">💬</span><span class="sb-tx">Suporte</span></button>
      <a class="sb-item" href="/logout"><span class="sb-ic">⏻</span><span class="sb-tx">Sair</span></a>
      <div class="sb-version">Versão __APP_VERSION__</div>
    </div>
  </aside>

  <!-- ══ CONTEÚDO ══ -->
  <div class="content">
    <header class="topbar">
      <button class="sb-toggle" onclick="toggleSidebar()" title="Menu">☰</button>
      <div class="tb-title" id="pageTitle">Dashboard</div>
      <div class="conn"><span class="dot" id="dot"></span><span id="connlbl">conectando…</span></div>
    </header>

    <!-- PÁGINA: DASHBOARD -->
    <div class="page" id="page-dashboard">
      <div id="dashContent"></div>
    </div>

    <!-- PÁGINA: IMPRESSORAS (a farm) -->
    <div class="page" id="page-printers" style="display:none">
      <div class="printers-head">
        <button class="kbtn kbtn-primary" onclick="openAdd()">＋ Adicionar impressora</button>
        <button class="kbtn" onclick="enterKiosk()">⛶ Painel de parede</button>
        <div class="ph-spacer"></div>
        <button class="kbtn" id="viewModeBtn" onclick="cycleViewMode()" title="Alternar visualização dos cards">▦ Completo</button>
        <button class="kbtn" id="colsBtn" onclick="cycleCols()" title="Colunas por linha">⊞ Auto</button>
      </div>
      <section class="overview" id="overview" style="display:none">
        <div class="fleet">
          <div class="eyebrow">Frota</div>
          <div class="fleet-counts" id="fleetCounts"></div>
          <div class="fleet-bar" id="fleetBar"></div>
        </div>
        <div class="eta-total" id="etaTotal"></div>
        <div class="farm-stats" id="farmStats"></div>
      </section>
      <main id="grid"><div class="empty"><b>Aguardando os primeiros dados</b>Verifique se as impressoras estão ligadas e conectadas à nuvem.</div></main>
    </div>

    <!-- PÁGINA: RELATÓRIOS -->
    <div class="page" id="page-reports" style="display:none">
      <div id="reportsContent"></div>
    </div>

    <!-- PÁGINA: PROJETOS -->
    <div class="page" id="page-projects" style="display:none">
      <div id="projectsContent"></div>
    </div>

    <!-- PÁGINA: CALCULADORA -->
    <div class="page" id="page-calc" style="display:none">
      <div id="calcContent"></div>
    </div>

    <!-- PÁGINA: CONFIGURAÇÕES -->
    <div class="page" id="page-settings" style="display:none">
      <div id="settingsContent"></div>
    </div>
  </div>
</div>

<div class="kexit" onclick="exitKiosk()">✕ sair do painel</div>
<div class="overlay" id="overlay" onclick="if(event.target===this)closeDetail()">
  <div class="modal" id="modal"></div>
</div>
<div class="overlay" id="addOverlay" onclick="if(event.target===this)closeAdd()">
  <div class="modal" id="addModal" style="width:min(560px,96vw)"></div>
</div>
<div class="overlay" id="reportOverlay" onclick="if(event.target===this)closeReports()">
  <div class="modal" id="reportModal" style="width:min(900px,96vw)"></div>
</div>
<div class="overlay" id="menuOverlay" onclick="if(event.target===this)closeMenu()">
  <div class="modal" id="menuModal" style="width:min(440px,96vw)"></div>
</div>
<div class="overlay" id="calcOverlay" onclick="if(event.target===this)closeCalc()">
  <div class="modal" id="calcModal" style="width:min(880px,96vw)"></div>
</div>
<div class="overlay" id="custoOverlay">
  <div class="modal" id="custoModal" style="width:min(480px,96vw)"></div>
</div>
<div class="overlay" id="abrirOverlay" onclick="if(event.target===this)fecharAbrir()">
  <div class="modal" id="abrirModal" style="width:min(460px,96vw)"></div>
</div>

<input type="file" id="logoFileInput" accept="image/png,image/jpeg" style="display:none" onchange="handleLogoFile(event)">

<script>
const STATES = {
  RUNNING:["printing","Imprimindo"], PREPARE:["printing","Preparando"],
  SLICING:["printing","Fatiando"], PAUSE:["paused","Pausada"],
  FINISH:["finish","Concluída"], FAILED:["failed","Falhou"],
  IDLE:["idle","Ociosa"],
};
const SPEED = {1:"Silencioso",2:"Padrão",3:"Sport",4:"Ludicrous"};
const STAGE = {
  "0":"imprimindo","1":"nivelando a mesa","2":"aquecendo a mesa","3":"varrendo eixos",
  "4":"trocando filamento","5":"pausa programada","6":"filamento acabou",
  "7":"aquecendo o bico","8":"calibrando extrusão","9":"escaneando a mesa",
  "10":"inspecionando 1ª camada","11":"identificando a mesa","12":"calibrando lidar",
  "13":"posicionando cabeçote","14":"limpando o bico","20":"calibrando",
};
const PIP = {printing:"var(--live)",paused:"var(--warn)",finish:"var(--done)",
  failed:"var(--fail)",idle:"#5b6b86"};
const SCOLOR = {printing:"#4f8cff",paused:"#ffcc44",finish:"#37d399",failed:"#ff5470",idle:"#5b6b86"};

/* histórico em memória (sessão): gráfico de temperatura + log de avisos por máquina */
const hist = {};
function recordHistory(name,p,online){
  if(!online) return;
  const h = hist[name] || (hist[name]={temps:[],alerts:[],codes:new Set(),lastSample:0});
  const now=Date.now();
  if(now-h.lastSample>=5000){
    h.lastSample=now;
    h.temps.push({t:now,n:p.nozzle_temper??null,b:p.bed_temper??null,c:p.chamber_temper??null});
    const cutoff=now-15*60000;
    while(h.temps.length && h.temps[0].t<cutoff) h.temps.shift();
  }
  const cur=new Set();
  if(Array.isArray(p.hms)) p.hms.forEach(x=>{ if(x&&(x.code||x.attr)) cur.add(hmsString(x)); });
  if(p.print_error&&p.print_error!==0) cur.add("Erro 0x"+(p.print_error>>>0).toString(16).toUpperCase());
  cur.forEach(code=>{ if(!h.codes.has(code)) h.alerts.unshift({t:now,code}); });
  h.codes=cur;
  if(h.alerts.length>60) h.alerts.length=60;
}
function tempChart(name){
  const h=hist[name];
  if(!h || h.temps.length<2) return '<div class="coll">Coletando dados… (alguns segundos)</div>';
  const W=420,H=130,pad=8, ts=h.temps;
  const t0=ts[0].t, t1=ts[ts.length-1].t, span=(t1-t0)||1;
  const vals=[]; ts.forEach(s=>{ if(s.n!=null)vals.push(s.n); if(s.b!=null)vals.push(s.b); });
  let mn=Math.min(...vals), mx=Math.max(...vals); if(!isFinite(mn)){mn=0;mx=10;}
  if(mx-mn<10) mx=mn+10;
  const X=t=>pad+(t-t0)/span*(W-2*pad);
  const Y=v=>H-pad-(v-mn)/(mx-mn)*(H-2*pad);
  const path=k=>{ let d=""; ts.forEach(s=>{ if(s[k]==null)return; d+=(d?"L":"M")+X(s.t).toFixed(1)+" "+Y(s[k]).toFixed(1)+" "; }); return d; };
  return `<svg viewBox="0 0 ${W} ${H}" width="100%" height="${H}" preserveAspectRatio="none">
    <line x1="${pad}" y1="${H-pad}" x2="${W-pad}" y2="${H-pad}" stroke="#1c2535"/>
    <path d="${path('b')}" fill="none" stroke="#4f8cff" stroke-width="2" stroke-linejoin="round"/>
    <path d="${path('n')}" fill="none" stroke="#ff7a3d" stroke-width="2" stroke-linejoin="round"/>
    <text x="${pad}" y="12" fill="#56627b" font-size="9" font-family="monospace">${Math.round(mx)}°</text>
    <text x="${pad}" y="${H-pad-2}" fill="#56627b" font-size="9" font-family="monospace">${Math.round(mn)}°</text>
  </svg>`;
}

function fmtTime(min){
  if(min==null||min<0) return "—";
  min=Math.round(min); const h=Math.floor(min/60), m=min%60;
  return h>0 ? `${h}h ${String(m).padStart(2,"0")}m` : `${m}m`;
}
const TIMEZONE="America/Sao_Paulo";  // fuso usado no "termina às" (mude se necessário)
function fmtETA(min){
  if(min==null||min<=0) return "—";
  return new Date(Date.now()+min*60000).toLocaleTimeString("pt-BR",
    {hour:"2-digit",minute:"2-digit",hour12:false,timeZone:TIMEZONE});
}
function objName(p){
  let n=(p.subtask_name||p.gcode_file||"").split("/").pop().replace(/\.(gcode|3mf)(\.3mf)?$/i,"");
  return n||"—";
}
function fanPct(v){ if(v==null) return null; let n=parseInt(v); if(isNaN(n)) return null;
  return n>15?Math.min(100,n):Math.round(n/15*100); }
function trayColor(c){ return (!c||c.length<6)?"#3a4150":"#"+c.slice(0,6); }
function hmsString(h){ const hx=n=>(n>>>0).toString(16).toUpperCase().padStart(4,"0");
  const a=h.attr||0,c=h.code||0;
  return `HMS_${hx((a>>>16)&0xFFFF)}_${hx(a&0xFFFF)}_${hx((c>>>16)&0xFFFF)}_${hx(c&0xFFFF)}`; }
function lightOn(p){ if(!Array.isArray(p.lights_report))return null;
  const l=p.lights_report.find(x=>x.node==="chamber_light"); return l?(l.mode==="on"):null; }
function wifiBars(p){ if(p.wifi_signal==null)return null; const n=parseInt(String(p.wifi_signal));
  if(isNaN(n))return null; return n>=-50?4:n>=-60?3:n>=-67?2:1; }
function stageText(p){ const s=String(p.stg_cur);
  if(s==="0") return `imprimindo · camada ${p.layer_num??"–"}`;
  return STAGE[s]||null; }

function tempChip(cl,val,target,hot){
  if(val==null) return "";
  const t = (target!=null&&target>0)?`<small>/${Math.round(target)}°</small>`:"";
  return `<div class="chip ${hot?"hot":""}"><span class="cl">${cl}</span><span class="vv">${Math.round(val)}°${t}</span></div>`;
}
function amsHtml(p){
  const units=p.ams&&p.ams.ams, now=p.ams?parseInt(p.ams.tray_now):-1, slots=[];
  if(Array.isArray(units)){
    units.forEach((u,ui)=>(u.tray||[]).forEach((t,ti)=>{
      const gi=ui*4+parseInt(t.id??ti), empty=!t.tray_type;
      const col=empty?"transparent":trayColor(t.tray_color);
      const ty=empty?"vazio":t.tray_type;
      const rm=(t.remain!=null&&t.remain>=0)?`<span class="rm">${t.remain}%</span>`:"";
      slots.push(`<div class="slot${gi===now?" active":""}"><span class="sw" style="background:${col}"></span>${ty}${rm}</div>`);
    }));
  } else if(p.vt_tray&&p.vt_tray.tray_type){
    const t=p.vt_tray;
    slots.push(`<div class="slot"><span class="sw" style="background:${trayColor(t.tray_color)}"></span>${t.tray_type}</div>`);
  }
  return slots.length?`<div class="ams"><span class="lbl">Filamento</span>${slots.join("")}</div>`:"";
}
function alertHtml(p){
  const items=[];
  if(p.print_error&&p.print_error!==0) items.push("Erro 0x"+(p.print_error>>>0).toString(16).toUpperCase());
  if(Array.isArray(p.hms)) p.hms.forEach(h=>{ if(h&&(h.code||h.attr)) items.push(hmsString(h)); });
  if(!items.length) return "";
  return `<div class="alert"><span>⚠</span><span>${items.join(" · ")}</span><a href="https://wiki.bambulab.com/en/general/error-codes" target="_blank">ver código</a></div>`;
}

function tagsHtml(p, model, meta, name){
  let t=`<span class="tag">${model||"impressora"}</span>`;
  // IP da rede local (só faz sentido quando a impressora é local)
  const m=meta||{};
  const ip=(m.ip||"").trim();
  const isLan=(m.mode||"")==="lan";
  if(isLan && ip){
    t+=`<span class="tag tag-ip" title="Endereço na rede local">🌐 ${ip}</span>`;
  }
  // custo informado para esta impressão
  const c=printCosts[name];
  if(c && c.custo && !c.skip){
    // peso à esquerda do custo (só quando informado)
    if(c.peso_g){
      const matt=c.material?` · ${c.material}`:"";
      t+=`<span class="gram-chip" title="Filamento desta peça${matt}">⚖ ${c.peso_g}g</span>`;
    }
    const mat=c.material?` · ${c.material}`:"";
    const pes=c.peso_g?` ${c.peso_g}g`:"";
    t+=`<span class="cost-chip" title="Custo desta impressão${mat}${pes}">💰 ${money(c.custo)}</span>`;
  }
  const wb=wifiBars(p);
  if(wb!=null){
    let bars=""; for(let i=1;i<=4;i++) bars+=`<i class="${i<=wb?"on":""}" style="height:${3+i*2}px"></i>`;
    t+=`<span class="tag"><span class="wifi">${bars}</span></span>`;
  }
  return t;
}
function envHtml(p){
  const nozzle=p.nozzle_temper, bed=p.bed_temper, cham=p.chamber_temper;
  const fan=fanPct(p.cooling_fan_speed), light=lightOn(p);
  return tempChip("BICO",nozzle,p.nozzle_target_temper,nozzle>50)
    + tempChip("MESA",bed,p.bed_target_temper,false)
    + tempChip("CÂM",cham,null,false)
    + (fan!=null?`<div class="chip"><span class="cl">VENT</span><span class="vv">${fan}%</span></div>`:"")
    + (light!=null?`<div class="chip"><span class="cl">LUZ</span><span class="vv">${light?"on":"off"}</span></div>`:"")
    + `<div class="chip"><span class="cl">VEL</span><span class="vv">${SPEED[p.spd_lvl]||"—"}</span></div>`;
}

const cards = {};   // nome -> referências dos elementos (construído uma vez)
let dragName=null;
let draggingActive=false;

function buildCard(name){
  const root=document.createElement("article");
  root.className="card";
  root.dataset.name=name;
  root.innerHTML=`
    <div class="accent"></div>
    <div class="cardtools">
      <span class="grip" draggable="true" title="Arrastar para reordenar">⠿</span>
      <button class="ren" title="Renomear impressora">✎</button>
      <button class="del" title="Remover impressora">✕</button>
    </div>
    <div class="row1">
      <div class="chamber">
        <div class="plate"></div>
        <div class="fill"></div>
        <div class="scan"></div>
        <div class="read"><div class="pc">—</div><div class="ly"></div></div>
      </div>
      <div class="body">
        <div class="top">
          <div class="pid"><div class="pname"></div><div class="tags"></div></div>
          <div class="pill"></div>
        </div>
        <div class="job">
          <div class="stage" style="display:none"></div>
          <div class="obj"></div>
          <div class="pbar"><i></i></div>
        </div>
        <div class="metrics">
          <div class="m"><span class="k">Restante</span><span class="v"></span></div>
          <div class="m"><span class="k">Termina</span><span class="v"></span></div>
          <div class="m"><span class="k">Decorrido</span><span class="v"></span></div>
          <div class="m"><span class="k">Total est.</span><span class="v"></span></div>
        </div>
        <div class="env"></div>
      </div>
    </div>
    <div class="ams-wrap"></div>
    <div class="alert-wrap"></div>
    <div class="foot"></div>`;
  const q=s=>root.querySelector(s);
  const refs={root, fill:q(".fill"), scan:q(".scan"), pc:q(".pc"), ly:q(".ly"),
    pname:q(".pname"), tags:q(".tags"), pill:q(".pill"), stage:q(".stage"),
    obj:q(".obj"), pbar:q(".pbar i"), env:q(".env"), ams:q(".ams-wrap"),
    alert:q(".alert-wrap"), m:[...root.querySelectorAll(".metrics .v")],
    _tags:"", _env:"", _ams:"", _alert:"", camOn:false};
  refs.pname.textContent=name;
  root.addEventListener("click",(e)=>{ if(e.target.closest(".cardtools")) return; openDetail(name); });
  q(".del").addEventListener("click",(e)=>{ e.stopPropagation(); removePrinter(name); });
  q(".ren").addEventListener("click",(e)=>{ e.stopPropagation(); renamePrinter(name); });
  const grip=q(".grip");
  grip.addEventListener("dragstart",(e)=>{ draggingActive=true; dragName=name;
    root.classList.add("dragging"); e.dataTransfer.effectAllowed="move";
    e.dataTransfer.setData("text/plain",name); });
  grip.addEventListener("dragend",()=>{ root.classList.remove("dragging");
    draggingActive=false; dragName=null; saveOrder(); });
  root.addEventListener("dragover",(e)=>{
    if(!dragName||dragName===name) return;
    e.preventDefault();
    const dr=cards[dragName] && cards[dragName].root; if(!dr) return;
    const rect=root.getBoundingClientRect();
    if((e.clientY-rect.top)>rect.height/2) root.after(dr); else root.before(dr);
  });
  cards[name]=refs;
  return refs;
}

function setHTML(ref,key,html){ if(ref[key]!==html){ ref[key]=html; return true;} return false; }

function updateCard(name,st){
  const r=cards[name]||buildCard(name);
  const meta=st._meta||{}, p=st.print||{}, online=meta.online;
  // Mostra o apelido se houver, senão o nome técnico
  const disp=(meta.apelido||"").trim()||name;
  if(r.pname && r.pname.textContent!==disp) r.pname.textContent=disp;
  const [cls,label]=STATES[p.gcode_state]||["idle",p.gcode_state||"Ociosa"];
  const scls=online?cls:"idle";
  const pct=Math.max(0,Math.min(100,Math.round(p.mc_percent??0)));
  const fillH=scls==="finish"?100:pct;
  const remain=p.mc_remaining_time;
  const totalEst=(pct>0&&pct<100&&remain!=null)?remain/(1-pct/100):null;
  const elapsed=totalEst!=null?totalEst-remain:null;
  const stage=online?stageText(p):null;
  const name2=objName(p);

  const newCls="card s-"+scls+(online?"":" offline");
  if(r.root.className!==newCls) r.root.className=newCls;
  r.fill.style.height=fillH+"%";
  r.scan.style.bottom=fillH+"%";
  r.pc.textContent=online?pct+"%":"—";
  r.ly.textContent=p.layer_num!=null?`camada ${p.layer_num}/${p.total_layer_num??"–"}`:"";
  if(setHTML(r,"_tags",tagsHtml(p,meta.model,meta,name))) r.tags.innerHTML=r._tags;
  r.pill.textContent = meta.auth_error ? "⚠ Token expirado" : (online ? label : "Offline");
  r.pill.style.color = meta.auth_error ? "#ffb020" : "";
  r.pill.title = meta.auth_error ? "O token da conta expirou. Adicione a impressora de novo com um token novo." : "";
  if(stage){ r.stage.style.display=""; r.stage.textContent=stage; } else r.stage.style.display="none";
  if(r.obj.textContent!==name2){ r.obj.textContent=name2; r.obj.title=name2; }
  r.pbar.style.width=pct+"%";
  r.m[0].textContent=fmtTime(remain);
  r.m[1].textContent=fmtETA(remain);
  r.m[2].textContent=fmtTime(elapsed);
  r.m[3].textContent=fmtTime(totalEst);
  if(setHTML(r,"_env",envHtml(p))) r.env.innerHTML=r._env;
  if(setHTML(r,"_ams",amsHtml(p))) r.ams.innerHTML=r._ams;
  if(setHTML(r,"_alert",alertHtml(p))) r.alert.innerHTML=r._alert;
  recordHistory(name,p,online);
}

function renderOverview(printers){
  const ov=document.getElementById("overview");
  const names=Object.keys(printers);
  if(!names.length){ov.style.display="none";return;}
  ov.style.display="flex";
  const c={printing:0,paused:0,finish:0,failed:0,idle:0,offline:0};
  let longest=null;
  for(const n of names){
    const st=printers[n], p=st.print||{}, online=(st._meta||{}).online;
    if(!online){c.offline++; continue;}
    const [cls]=STATES[p.gcode_state]||["idle"]; c[cls]++;
    if(cls==="printing" && p.mc_remaining_time!=null)
      longest=Math.max(longest??0,p.mc_remaining_time);
  }
  const counts=[
    ["Imprimindo",c.printing,PIP.printing],
    ["Ociosas",c.idle+c.offline,PIP.idle],
    ["Concluídas",c.finish,PIP.finish],
    ["Alertas",c.failed,PIP.failed],
  ];
  document.getElementById("fleetCounts").innerHTML=counts.map(([k,v,col])=>
    `<span class="fc"><span class="pip" style="background:${col}"></span>${k} <b>${v}</b></span>`).join("");
  const total=names.length||1;
  const seg=[["printing",PIP.printing],["paused",PIP.paused],["finish",PIP.finish],
    ["failed",PIP.failed],["idle",PIP.idle],["offline","#2a3344"]];
  document.getElementById("fleetBar").innerHTML=seg.map(([k,col])=>{
    const w=(c[k]/total*100)||0; return w>0?`<i style="width:${w}%;background:${col}"></i>`:"";}).join("");
  document.getElementById("etaTotal").innerHTML = longest!=null
    ? `<span class="eyebrow">Tudo pronto em</span><span class="v" style="font-family:'JetBrains Mono';font-size:1.05rem">${fmtTime(longest)}</span>`
    : `<span class="eyebrow">Frota</span><span class="v" style="font-family:'JetBrains Mono';font-size:1.05rem;color:var(--muted)">em repouso</span>`;
}

let lastData={};
let lastOrder=[];
let scheduled=false;
function applyUpdate(){
  scheduled=false;
  const printers=lastData;
  const grid=document.getElementById("grid");
  const names=Object.keys(printers);
  if(!names.length){
    grid.innerHTML='<div class="empty"><b>Nenhuma impressora ainda</b>Clique em “＋ Adicionar impressora” no topo para começar.</div>';
    for(const k in cards) delete cards[k];
    renderOverview(printers); return;
  }
  const emptyEl=grid.querySelector(".empty"); if(emptyEl) emptyEl.remove();
  // remove cards de impressoras que saíram
  for(const n of Object.keys(cards)){
    if(!(n in printers)){ cards[n].root.remove(); delete cards[n]; }
  }
  // sequência final = ordem do servidor (+ qualquer uma que falte no fim)
  const seq = lastOrder.filter(n=>n in printers);
  for(const n of names){ if(!seq.includes(n)) seq.push(n); }
  // cria/atualiza cada card na ordem e força a posição no DOM
  for(const n of seq){
    updateCard(n,printers[n]);
    if(!draggingActive) grid.appendChild(cards[n].root);   // mover para o fim em sequência = ordena
  }
  renderOverview(printers);
  if(openName) refreshDetail();
}
function render(printers, order){
  lastData=printers;
  lastOrder=(order&&order.length)?order:Object.keys(printers);
  if(!scheduled){ scheduled=true; requestAnimationFrame(applyUpdate); }
}

/* ── Modal de detalhe ─────────────────────────────────── */
let openName=null;
function openDetail(name){
  openName=name;
  buildModal();
  document.getElementById("overlay").classList.add("open");
  refreshDetail();
}
function closeDetail(){
  openName=null;
  modalCam.on=false;
  document.getElementById("overlay").classList.remove("open");
  document.getElementById("modal").innerHTML="";
}
function buildModal(){
  const st=lastData[openName]||{}, meta=st._meta||{};
  const hasCam = meta.has_camera;
  const camPanel = hasCam
    ? `<div class="ph">Câmera <button class="modal-cam-btn" id="m_cam_btn" onclick="toggleModalCamera()">📷 Ligar</button></div>
       <div class="cam" id="m_cam"><div class="cam-off">Câmera desligada.<br>Clique em "Ligar" para ver.</div></div>`
    : `<div class="ph">Câmera</div>
       <div class="cam"><div class="cam-off">Câmera indisponível para esta impressora.</div></div>`;
  const dispName=(meta.apelido||"").trim()||openName;
  const lanIp=((meta.mode||"")==="lan" && (meta.ip||"").trim()) ? (meta.ip||"").trim() : "";
  const ipTag=lanIp?`<span class="tag tag-ip" style="margin-left:.5rem">🌐 ${lanIp}</span>`:"";
  document.getElementById("modal").innerHTML=`
    <div class="mhead">
      <div>
        <div class="mh-name"><span id="m_name">${dispName}</span><span class="pill" id="m_pill"></span>${ipTag}</div>
        <div class="mh-obj" id="m_obj"></div>
      </div>
      <div class="mclose" onclick="closeDetail()">✕</div>
    </div>
    <div class="mgrid">
      <div class="panel">${camPanel}</div>
      <div class="panel">
        <div class="ph">Temperatura · últimos 15 min</div>
        <div class="legend"><span><i style="background:#ff7a3d"></i>Bico</span><span><i style="background:#4f8cff"></i>Mesa</span></div>
        <div class="chart" id="m_chart"></div>
        <div class="bigtemps" id="m_bigtemps"></div>
      </div>
    </div>
    <div class="mdetail" id="m_detail"></div>
    <div class="alog"><div class="ph aph">Histórico de avisos (sessão)</div><div id="m_alog"></div></div>`;
  modalCam.on=false;  // começa desligada ao abrir
}

const modalCam={on:false, tick:0};
function toggleModalCamera(){
  const name=openName;
  const box=document.getElementById("m_cam");
  const btn=document.getElementById("m_cam_btn");
  if(!box||!btn) return;
  modalCam.on=!modalCam.on;
  if(modalCam.on){
    btn.textContent="🎥 Desligar";
    btn.classList.add("on");
    box.innerHTML=`<img alt="câmera ao vivo" style="width:100%;height:100%;object-fit:contain"><div class="cam-load">Conectando…</div>`;
    const img=box.querySelector("img");
    const loadNext=()=>{
      if(!modalCam.on || openName!==name) return;
      const next=new Image();
      next.onload=()=>{
        if(!modalCam.on || openName!==name) return;
        img.src=next.src;
        const ld=box.querySelector(".cam-load"); if(ld) ld.remove();
        setTimeout(loadNext, 50);  // busca a próxima quase imediatamente
      };
      next.onerror=()=>{
        if(!modalCam.on || openName!==name) return;
        const ld=box.querySelector(".cam-load");
        if(ld) ld.textContent="Aguardando câmera…";
        setTimeout(loadNext, 1200);
      };
      next.src=`/camera/${encodeURIComponent(name)}?t=${Date.now()}_${modalCam.tick++}`;
    };
    loadNext();
  } else {
    btn.textContent="📷 Ligar";
    btn.classList.remove("on");
    box.innerHTML=`<div class="cam-off">Câmera desligada.<br>Clique em "Ligar" para ver.</div>`;
  }
}
function metric(k,v){ return `<div class="m"><span class="k">${k}</span><span class="v">${v}</span></div>`; }
function refreshDetail(){
  if(openName==null) return;
  const st=lastData[openName];
  if(!st){ closeDetail(); return; }
  const p=st.print||{}, meta=st._meta||{}, online=meta.online;
  const [cls,label]=STATES[p.gcode_state]||["idle",p.gcode_state||"Ociosa"];
  const scls=online?cls:"idle", col=SCOLOR[scls];
  const pct=Math.max(0,Math.min(100,Math.round(p.mc_percent??0)));
  const remain=p.mc_remaining_time;
  const totalEst=(pct>0&&pct<100&&remain!=null)?remain/(1-pct/100):null;
  const elapsed=totalEst!=null?totalEst-remain:null;
  const set=(id,html)=>{const e=document.getElementById(id); if(e)e.innerHTML=html;};

  const pill=document.getElementById("m_pill");
  if(pill){ pill.textContent=online?label:"Offline";
    pill.style.cssText=`border:1px solid ${col};color:${col};background:${col}1a`; }
  set("m_obj", objName(p));
  set("m_chart", tempChart(openName));
  set("m_bigtemps",
     `<div><span>BICO</span><b style="color:#ff7a3d">${p.nozzle_temper!=null?Math.round(p.nozzle_temper)+"°":"—"}</b></div>`
    +`<div><span>MESA</span><b>${p.bed_temper!=null?Math.round(p.bed_temper)+"°":"—"}</b></div>`
    +(p.chamber_temper!=null?`<div><span>CÂMARA</span><b>${Math.round(p.chamber_temper)}°</b></div>`:"")
    +(fanPct(p.cooling_fan_speed)!=null?`<div><span>VENT</span><b>${fanPct(p.cooling_fan_speed)}%</b></div>`:""));
  set("m_detail",
     metric("Progresso",pct+"%")
    +metric("Restante",fmtTime(remain))
    +metric("Termina às",fmtETA(remain))
    +metric("Decorrido",fmtTime(elapsed))
    +metric("Total est.",fmtTime(totalEst))
    +metric("Camada",(p.layer_num!=null?p.layer_num:"—")+" / "+(p.total_layer_num??"—")));
  const h=hist[openName]||{alerts:[]};
  set("m_alog", h.alerts.length
    ? h.alerts.map(a=>`<div class="ev"><time>${new Date(a.t).toLocaleTimeString("pt-BR",{hour:"2-digit",minute:"2-digit",hour12:false,timeZone:TIMEZONE})}</time><span class="code">${a.code}</span></div>`).join("")
    : `<div class="none">Nenhum aviso registrado nesta sessão.</div>`);
}

/* ── Modo painel de parede (kiosk) ────────────────────── */
let wakeLock=null;
/* ── Menu principal ───────────────────────────────────── */
function openMenu(){
  renderMenu();
  document.getElementById("menuOverlay").classList.add("open");
}
function closeMenu(){
  document.getElementById("menuOverlay").classList.remove("open");
}
function renderMenu(){
  const vm=VIEW_MODES[viewModeIdx];
  const nCols=COLS_OPTIONS[colsIdx];
  const colsLabel=nCols===0?"Automático":`${nCols} ${nCols===1?'coluna':'colunas'}`;
  document.getElementById("menuModal").innerHTML=`
    <div class="menu-head"><b>☰ Menu</b><div class="mclose" onclick="closeMenu()">✕</div></div>
    <div class="menu-body">
      <button class="menu-item primary" onclick="closeMenu();openAdd()">
        <span class="mi-ic">＋</span>
        <span class="mi-tx"><b>Adicionar impressora</b><small>Conectar uma nova impressora</small></span>
      </button>
      <button class="menu-item" onclick="closeMenu();openReports()">
        <span class="mi-ic">📊</span>
        <span class="mi-tx"><b>Relatórios</b><small>Histórico e custos de impressão</small></span>
      </button>
      <button class="menu-item" onclick="closeMenu();openCalc()">
        <span class="mi-ic">🧮</span>
        <span class="mi-tx"><b>Calculadora de custo</b><small>Quanto custa e por quanto vender</small></span>
      </button>
      <button class="menu-item" onclick="closeMenu();enterKiosk()">
        <span class="mi-ic">⛶</span>
        <span class="mi-tx"><b>Painel de parede</b><small>Tela cheia para TV / monitor</small></span>
      </button>
      <div class="menu-sep">Visualização</div>
      <button class="menu-item" onclick="cycleViewMode();renderMenu()">
        <span class="mi-ic">▦</span>
        <span class="mi-tx"><b>Modo de visualização</b><small>Toque para alternar</small></span>
        <span class="mi-val">${vm.label}</span>
      </button>
      <button class="menu-item" onclick="cycleCols();renderMenu()">
        <span class="mi-ic">⊞</span>
        <span class="mi-tx"><b>Cards por linha</b><small>Toque para alternar</small></span>
        <span class="mi-val">${colsLabel}</span>
      </button>
      <div class="menu-sep">Personalização</div>
      <button class="menu-item" onclick="openLogoPicker()">
        <span class="mi-ic">🖼️</span>
        <span class="mi-tx"><b>Trocar logo</b><small>Use a logo da sua empresa</small></span>
      </button>
      <div class="menu-sep">Conta</div>
      <button class="menu-item" onclick="closeMenu();changePassword()">
        <span class="mi-ic">🔑</span>
        <span class="mi-tx"><b>Trocar senha</b><small>Alterar a senha de acesso</small></span>
      </button>
      <div class="menu-sep">Ajuda</div>
      <button class="menu-item" onclick="abrirWhatsapp('suporte')">
        <span class="mi-ic">💬</span>
        <span class="mi-tx"><b>Suporte</b><small>Falar com o suporte no WhatsApp</small></span>
      </button>
      <button class="menu-item" onclick="abrirWhatsapp('licenca')">
        <span class="mi-ic">🛒</span>
        <span class="mi-tx"><b>Comprar licença</b><small>Adquirir ou renovar sua licença</small></span>
      </button>
    </div>`;
}

/* ── Navegação entre páginas ──────────────────────────── */
const PAGES={dashboard:"Dashboard",printers:"Impressoras",projects:"Projetos",
  reports:"Relatórios",calc:"Calculadora",settings:"Configurações"};
let currentPage="dashboard";
function nav(page){
  currentPage=page;
  Object.keys(PAGES).forEach(p=>{
    const el=document.getElementById("page-"+p);
    if(el) el.style.display = (p===page)?"":"none";
  });
  document.querySelectorAll(".sb-item[data-page]").forEach(b=>{
    b.classList.toggle("active", b.dataset.page===page);
  });
  const t=document.getElementById("pageTitle");
  if(t) t.textContent=PAGES[page]||"";
  document.querySelector(".app")?.classList.remove("sb-open");
  if(page==="dashboard") renderDash();
  else if(page==="printers") applyViewMode();
  else if(page==="reports") mountReports();
  else if(page==="projects") renderProjects();
  else if(page==="calc") mountCalc();
  else if(page==="settings") renderSettings();
}
function toggleSidebar(){
  document.querySelector(".app")?.classList.toggle("sb-open");
}

/* ── Dashboard (home) ─────────────────────────────────── */
let dashPeriod="mes";
async function renderDash(){
  const box=document.getElementById("dashContent");
  if(!box) return;
  const printers=Object.entries(lastData||{});
  let imprimindo=0, ociosas=0, erro=0, offline=0;
  for(const [n,st] of printers){
    const meta=st._meta||{}, p=st.print||{};
    if(!meta.online){ offline++; continue; }
    const gs=p.gcode_state;
    if(gs==="RUNNING"||gs==="PREPARE") imprimindo++;
    else if(gs==="FAILED") erro++;
    else ociosas++;
  }
  const totalP=printers.length;
  let rep=null;
  try{ rep=await (await fetch("/api/report?period="+dashPeriod)).json(); }catch(_){}

  box.innerHTML=`
    <div class="dash-grid">
      <div class="dash-kpis">
        <div class="kpi k-live">
          <div class="kpi-ic">🖨️</div>
          <div class="kpi-v"><span data-count="${imprimindo}">0</span><span style="font-size:1rem;color:var(--muted)">/${totalP}</span></div>
          <div class="kpi-k">Imprimindo agora</div>
          <div class="kpi-sub">${ociosas} ociosas · ${erro} erro · ${offline} offline</div>
        </div>
        <div class="kpi k-ok">
          <div class="kpi-ic">✓</div>
          <div class="kpi-v">${rep&&rep.success_rate!=null?`<span data-count="${rep.success_rate}" data-suffix="%">0</span>`:"—"}</div>
          <div class="kpi-k">Taxa de sucesso</div>
          <div class="kpi-sub">${rep?rep.success+" ok · "+rep.failed+" falhas":""}</div>
        </div>
        <div class="kpi k-fila">
          <div class="kpi-ic">⚖</div>
          <div class="kpi-v">${rep?`<span data-count="${rep.total_peso_g||0}" data-peso="1">0</span>`:"—"}</div>
          <div class="kpi-k">Filamento usado</div>
          <div class="kpi-sub">${rep?rep.total+" impressões":""}</div>
        </div>
        <div class="kpi k-cost">
          <div class="kpi-ic">💰</div>
          <div class="kpi-v" style="font-size:1.7rem">${rep?`<span data-count="${rep.total_custo||0}" data-money="1">0</span>`:"—"}</div>
          <div class="kpi-k">Custo total</div>
          <div class="kpi-sub">${rep?rep.total_hours+"h de impressão":""}</div>
        </div>
      </div>

      ${renderDestaques(rep)}

      <div class="dash-period">
        ${["dia","semana","mes","ano","tudo"].map(p=>
          `<button class="rep-pbtn ${p===dashPeriod?'active':''}" onclick="setDashPeriod('${p}')">${periodLabel(p)}</button>`
        ).join("")}
      </div>

      <div class="dash-box">
        <h3>Produção — ${periodLabel(dashPeriod)}</h3>
        ${renderProdChart(rep)}
      </div>

      <div class="dash-box">
        <h3>Impressoras — ao vivo</h3>
        <div class="pcard-grid">${renderPrinterCards(printers)}</div>
      </div>

      <div class="dash-cols">
        <div class="dash-box">
          <h3>Resultado do período</h3>
          ${renderDashDonut(rep)}
        </div>
        <div class="dash-box">
          <h3>Materiais usados</h3>
          ${renderMateriais(rep)}
        </div>
      </div>

      <div class="dash-box">
        <h3>Custo por impressora (${periodLabel(dashPeriod)})</h3>
        ${renderCostBars(rep)}
      </div>
    </div>`;
  animateCounts();
  animateProgRings();
}
function setDashPeriod(p){ dashPeriod=p; renderDash(); }
function periodLabel(p){ return {dia:"Hoje",semana:"Semana",mes:"Mês",ano:"Ano",tudo:"Tudo"}[p]||p; }
function fmtPeso(g){ g=g||0; return g>=1000?(g/1000).toFixed(2)+" kg":Math.round(g)+" g"; }

/* números que sobem animados */
function animateCounts(){
  document.querySelectorAll("[data-count]").forEach(el=>{
    const alvo=parseFloat(el.dataset.count)||0;
    const money=el.dataset.money, peso=el.dataset.peso, suf=el.dataset.suffix||"";
    const dur=650, t0=performance.now();
    const fmt=v=>{
      if(money) return "R$ "+v.toLocaleString("pt-BR",{minimumFractionDigits:2,maximumFractionDigits:2});
      if(peso) return v>=1000?(v/1000).toFixed(2)+" kg":Math.round(v)+" g";
      return Math.round(v)+suf;
    };
    const step=now=>{
      const t=Math.min(1,(now-t0)/dur);
      const e=1-Math.pow(1-t,3);      // easing suave
      el.textContent=fmt(alvo*e);
      if(t<1) requestAnimationFrame(step);
      else el.textContent=fmt(alvo);
    };
    requestAnimationFrame(step);
  });
}

/* destaques / ranking */
function renderDestaques(rep){
  if(!rep||!rep.destaques) return "";
  const d=rep.destaques;
  const cards=[];
  if(d.mais_produtiva) cards.push(`<div class="hl-card"><div class="hl-ic">🏆</div>
    <div class="hl-tx"><small>Mais produtiva</small><b>${d.mais_produtiva.nome}</b>
    <span>${d.mais_produtiva.total} impressões</span></div></div>`);
  if(d.melhor_taxa) cards.push(`<div class="hl-card"><div class="hl-ic">🎯</div>
    <div class="hl-tx"><small>Melhor taxa</small><b>${d.melhor_taxa.nome}</b>
    <span>${d.melhor_taxa.taxa}% de sucesso</span></div></div>`);
  if(d.dia_pico){
    const dt=d.dia_pico.dia.split("-").reverse().slice(0,2).join("/");
    cards.push(`<div class="hl-card"><div class="hl-ic">📈</div>
      <div class="hl-tx"><small>Dia de pico</small><b>${dt}</b>
      <span>${d.dia_pico.total} impressões</span></div></div>`);
  }
  if(!cards.length) return "";
  return `<div class="hl-grid">${cards.join("")}</div>`;
}

/* gráfico de produção ao longo do tempo (barras por dia) */
function renderProdChart(rep){
  if(!rep||!rep.serie||!rep.serie.length)
    return `<div class="dash-empty"><b>Sem produção no período</b>O gráfico aparece conforme as impressões terminam.</div>`;
  const serie=rep.serie;
  const max=Math.max(...serie.map(s=>s.total),1);
  const W=Math.max(serie.length*44, 300), H=180, pad=28;
  const bw=Math.min(32, (W-pad)/serie.length-8);
  let bars="", labels="", ticks="";
  serie.forEach((s,i)=>{
    const x=pad+i*((W-pad)/serie.length)+((W-pad)/serie.length-bw)/2;
    const okH=(s.success/max)*(H-pad-20);
    const failH=(s.failed/max)*(H-pad-20);
    const y0=H-pad;
    bars+=`<rect x="${x}" y="${y0-okH}" width="${bw}" height="${okH}" rx="3" fill="#37d67a" class="prod-bar" style="--d:${i*0.04}s"/>`;
    if(failH>0) bars+=`<rect x="${x}" y="${y0-okH-failH}" width="${bw}" height="${failH}" rx="3" fill="#ff5470" class="prod-bar" style="--d:${i*0.04}s"/>`;
    if(s.total>0) labels+=`<text x="${x+bw/2}" y="${y0-okH-failH-5}" text-anchor="middle" fill="#8a96a8" font-size="10" font-family="monospace">${s.total}</text>`;
    const dl=s.dia.split("-").reverse().slice(0,2).join("/");
    // mostra rótulo a cada N para não poluir
    if(serie.length<=12 || i%Math.ceil(serie.length/10)===0)
      ticks+=`<text x="${x+bw/2}" y="${H-8}" text-anchor="middle" fill="#5a6473" font-size="9">${dl}</text>`;
  });
  return `<div class="prod-wrap"><svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMinYMid meet" style="width:100%;height:${H}px">
    ${bars}${labels}${ticks}
  </svg>
  <div class="prod-legend"><span><i style="background:#37d67a"></i> concluídas</span>
    <span><i style="background:#ff5470"></i> falhas</span></div></div>`;
}

/* cards de impressora ao vivo com anel de progresso */
function renderPrinterCards(printers){
  if(!printers.length) return `<div class="dash-empty" style="grid-column:1/-1"><b>Nenhuma impressora conectada</b>Adicione impressoras na aba Impressoras.</div>`;
  return printers.map(([name,st])=>{
    const meta=st._meta||{}, p=st.print||{};
    const disp=(meta.apelido||"").trim()||name;
    const online=meta.online, gs=p.gcode_state;
    let cls="off", txt="Offline", cor="#5a6473";
    if(online){
      if(gs==="RUNNING"||gs==="PREPARE"){ cls="run"; txt="Imprimindo"; cor="#4f8cff"; }
      else if(gs==="FAILED"){ cls="err"; txt="Erro"; cor="#ff5470"; }
      else if(gs==="PAUSE"){ cls="pause"; txt="Pausada"; cor="#e0a94f"; }
      else{ cls="on"; txt="Ociosa"; cor="#37d67a"; }
    }
    const pct=Math.max(0,Math.min(100,Math.round(p.mc_percent??0)));
    const obj=((p.subtask_name||"")+"").split("/").pop().replace(/\.(gcode|3mf)$/i,"");
    const remain=p.mc_remaining_time;
    const rodando=(gs==="RUNNING"||gs==="PREPARE");
    const R=32, C=2*Math.PI*R;
    const off=C*(1-pct/100);
    return `<div class="pcard ${cls}">
      <div class="pcard-ring">
        <svg width="80" height="80" viewBox="0 0 80 80">
          <circle cx="40" cy="40" r="${R}" fill="none" stroke="#1a2333" stroke-width="6"/>
          <circle cx="40" cy="40" r="${R}" fill="none" stroke="${cor}" stroke-width="6"
            stroke-linecap="round" stroke-dasharray="${C}"
            stroke-dashoffset="${C}" data-ring="${off}"
            transform="rotate(-90 40 40)" class="pcard-arc"/>
          <text x="40" y="40" text-anchor="middle" dominant-baseline="central"
            fill="#e8edf5" font-size="16" font-weight="700" font-family="monospace">${rodando?pct+"%":""}</text>
          ${!rodando?`<text x="40" y="40" text-anchor="middle" dominant-baseline="central" font-size="20">${cls==="on"?"✓":cls==="err"?"✕":cls==="pause"?"⏸":"○"}</text>`:""}
        </svg>
      </div>
      <div class="pcard-info">
        <div class="pcard-name">${disp}</div>
        <div class="pcard-status" style="color:${cor}"><span class="pcard-dot" style="background:${cor}"></span>${txt}</div>
        ${rodando&&obj?`<div class="pcard-obj" title="${obj}">${obj}</div>`:""}
        ${rodando&&remain!=null?`<div class="pcard-time">⏱ ${fmtTime(remain)} restante</div>`:""}
      </div>
    </div>`;
  }).join("");
}
/* anima os anéis dos cards de impressora */
function animateProgRings(){
  requestAnimationFrame(()=>{
    document.querySelectorAll(".pcard-arc[data-ring]").forEach(el=>{
      el.style.strokeDashoffset=el.dataset.ring;
    });
  });
}

/* distribuição por material (rosca) */
function renderMateriais(rep){
  if(!rep||!rep.materiais||!rep.materiais.length)
    return `<div class="dash-empty">Informe o material nas impressões para ver a distribuição.</div>`;
  const mats=rep.materiais;
  const cores={PLA:"#4f8cff",PETG:"#37d67a",ABS:"#e0a94f",TPU:"#c77dff"};
  const outras=["#ff5470","#00d4d4","#a0a0ff"];
  const totalPeso=mats.reduce((a,m)=>a+m.peso_g,0)||1;
  const R=52, C=2*Math.PI*R;
  let ang=0, arcs="", leg="";
  mats.forEach((m,i)=>{
    const frac=m.peso_g/totalPeso;
    const len=C*frac;
    const cor=cores[m.material]||outras[i%outras.length];
    arcs+=`<circle cx="65" cy="65" r="${R}" fill="none" stroke="${cor}" stroke-width="15"
      stroke-dasharray="${len} ${C-len}" stroke-dashoffset="${-ang}"
      transform="rotate(-90 65 65)"/>`;
    ang+=len;
    leg+=`<div><i style="background:${cor}"></i> ${m.material}
      <span style="color:var(--faint)">${fmtPeso(m.peso_g)} · ${m.count}×</span></div>`;
  });
  return `<div class="mini-donut">
    <svg width="130" height="130" viewBox="0 0 130 130">${arcs}
      <text x="65" y="60" text-anchor="middle" fill="#e8edf5" font-size="15" font-weight="700" font-family="monospace">${fmtPeso(totalPeso)}</text>
      <text x="65" y="78" text-anchor="middle" fill="#8a96a8" font-size="10">total</text>
    </svg>
    <div class="md-legend">${leg}</div>
  </div>`;
}
function renderDashDonut(rep){
  if(!rep||!rep.total) return `<div class="dash-empty"><b>Sem impressões no período</b>Os dados aparecem conforme as impressões terminam.</div>`;
  const ok=rep.success, fail=rep.failed, tot=rep.total;
  const okPct=tot?ok/tot:0, R=52, C=2*Math.PI*R, okLen=C*okPct;
  return `<div class="mini-donut">
    <svg width="130" height="130" viewBox="0 0 130 130">
      <circle cx="65" cy="65" r="${R}" fill="none" stroke="#1a2333" stroke-width="14"/>
      <circle cx="65" cy="65" r="${R}" fill="none" stroke="#37d67a" stroke-width="14"
        stroke-dasharray="${okLen} ${C-okLen}" stroke-dashoffset="${C*0.25}"
        transform="rotate(-90 65 65)" stroke-linecap="round"/>
      <text x="65" y="60" text-anchor="middle" fill="#e8edf5" font-size="26" font-weight="700"
        font-family="monospace">${rep.success_rate!=null?rep.success_rate:0}%</text>
      <text x="65" y="80" text-anchor="middle" fill="#8a96a8" font-size="11">sucesso</text>
    </svg>
    <div class="md-legend">
      <div><i style="background:#37d67a"></i> ${ok} concluídas</div>
      <div><i style="background:#ff5470"></i> ${fail} falhas</div>
      <div><i style="background:#4f8cff"></i> ${rep.total_hours}h total</div>
      <div><i style="background:#e0a94f"></i> ${fmtPeso(rep.total_peso_g)}</div>
    </div>
  </div>`;
}
function renderCostBars(rep){
  if(!rep||!rep.by_printer||!rep.by_printer.length)
    return `<div class="dash-empty">Sem dados de custo no período.</div>`;
  const arr=rep.by_printer.map(d=>({name:d.printer, custo:d.custo||0, total:d.total}))
    .sort((a,b)=>b.custo-a.custo);
  const max=Math.max(...arr.map(d=>d.custo), 0.01);
  const cores=["#4f8cff","#37d67a","#e0a94f","#c77dff","#ff5470","#00d4d4"];
  return `<div class="dash-bars">`+arr.map((d,i)=>{
    const w=Math.max(2, d.custo/max*100);
    return `<div class="dbar">
      <div class="dbar-name" title="${d.name}">${d.name}</div>
      <div class="dbar-track">
        <div class="dbar-fill" style="width:${w}%;background:${cores[i%cores.length]}">
          ${d.custo>0?money(d.custo):""}</div>
      </div>
    </div>`;
  }).join("")+`</div>`;
}

/* ── Configurações (página) ───────────────────────────── */
function renderSettings(){
  const box=document.getElementById("settingsContent");
  if(!box) return;
  box.innerHTML=`
    <div class="settings-grid">
      <button class="set-card" onclick="openLogoPicker()">
        <span class="set-ic">🖼️</span>
        <span class="set-tx"><b>Trocar logo</b><small>Use a logo da sua empresa</small></span></button>
      <button class="set-card" onclick="changePassword()">
        <span class="set-ic">🔑</span>
        <span class="set-tx"><b>Trocar senha</b><small>Alterar a senha de acesso</small></span></button>
      <button class="set-card" onclick="openCalcConfig()">
        <span class="set-ic">💲</span>
        <span class="set-tx"><b>Preços padrão</b><small>Filamento, energia e margem</small></span></button>
      <button class="set-card" onclick="abrirWhatsapp('licenca')">
        <span class="set-ic">🛒</span>
        <span class="set-tx"><b>Comprar / renovar licença</b><small>Falar no WhatsApp</small></span></button>
      <button class="set-card" onclick="abrirWhatsapp('suporte')">
        <span class="set-ic">💬</span>
        <span class="set-tx"><b>Suporte</b><small>Precisa de ajuda?</small></span></button>
      <button class="set-card" onclick="checkUpdate(false)">
        <span class="set-ic">🔄</span>
        <span class="set-tx"><b>Verificar atualização</b><small>Versão atual: __APP_VERSION__</small></span></button>
    </div>`;
}
function openCalcConfig(){ nav('calc'); }

/* ── Gerenciador de projetos ──────────────────────────── */
let projLocal="local", projPath="", projCloudOk=false;
async function renderProjects(){
  const box=document.getElementById("projectsContent");
  if(!box) return;
  // verifica config da nuvem
  try{
    const c=await (await fetch("/api/projetos/config")).json();
    projCloudOk=c.cloud_ok; window._cloudDir=c.cloud_dir||"";
  }catch(_){ projCloudOk=false; }
  await loadProjects();
}
async function loadProjects(){
  const box=document.getElementById("projectsContent");
  if(!box) return;
  let data;
  try{
    data=await (await fetch(`/api/projetos/list?local=${projLocal}&path=${encodeURIComponent(projPath)}`)).json();
  }catch(_){ box.innerHTML=`<div class="dash-empty">Erro ao carregar.</div>`; return; }

  if(!data.ok && data.error==="cloud_nao_configurada"){
    box.innerHTML=projHeader()+`
      <div class="proj-cloud-setup">
        <div class="pcs-ic">☁️</div>
        <h3>Configurar armazenamento em nuvem</h3>
        <p>Para salvar na nuvem automaticamente, informe uma pasta que já sincroniza
           com seu Google Drive, OneDrive ou Dropbox. Tudo que você salvar ali é
           enviado para a nuvem pelo próprio serviço.</p>
        <p class="pcs-ex">Exemplos:<br>
           <code>C:\\Users\\SeuNome\\Google Drive\\Projetos3D</code><br>
           <code>C:\\Users\\SeuNome\\OneDrive\\Projetos3D</code></p>
        <div class="pcs-row">
          <input id="cloudDirInput" placeholder="Cole o caminho da pasta aqui" value="${window._cloudDir||''}">
          <button class="kbtn kbtn-primary" onclick="saveCloudDir()">Salvar</button>
        </div>
        <button class="kbtn" onclick="projLocal='local';loadProjects()">← Voltar para o armazenamento local</button>
      </div>`;
    return;
  }
  if(!data.ok){ box.innerHTML=projHeader()+`<div class="dash-empty">${data.error||"Erro."}</div>`; return; }

  const crumbs=projBreadcrumb();
  const folders=data.folders.map(f=>`
    <div class="proj-item proj-folder" draggable="true"
      data-rel="${escq(f.rel)}" data-kind="folder"
      ondragstart="projDragStart(event,'${escq(f.rel)}')"
      ondragend="projDragEnd(event)"
      ondragover="projFolderOver(event)"
      ondragleave="projFolderLeave(event)"
      ondrop="projFolderDrop(event,'${escq(f.rel)}')"
      ondblclick="projOpen('${escq(f.rel)}')">
      <div class="pi-ic">📁</div>
      <div class="pi-name" title="${escq(f.name)}">${f.name}</div>
      <div class="pi-actions">
        <button onclick="event.stopPropagation();projRename('${escq(f.rel)}','${escq(f.name)}')" title="Renomear">✎</button>
        <button onclick="event.stopPropagation();projDelete('${escq(f.rel)}','${escq(f.name)}',true)" title="Excluir">🗑</button>
      </div>
    </div>`).join("");
  const files=data.files.map(f=>{
    const abrivel=["stl","3mf","obj"].includes(f.ext);
    return `
    <div class="proj-item proj-file ${abrivel?'proj-openable':''}" draggable="true"
      data-rel="${escq(f.rel)}" data-kind="file"
      ondragstart="projDragStart(event,'${escq(f.rel)}')"
      ondragend="projDragEnd(event)"
      ${abrivel?`onclick="projAbrir('${escq(f.rel)}','${escq(f.name)}')" title="Clique para abrir no fatiador"`:''}>
      <div class="pi-ic pi-ext ext-${f.ext}">${projIcon(f.ext)}</div>
      <div class="pi-name" title="${escq(f.name)}">${f.name}<span class="pi-size">${f.size}</span></div>
      <div class="pi-actions">
        <a href="/api/projetos/download?local=${projLocal}&path=${encodeURIComponent(f.rel)}" onclick="event.stopPropagation()" title="Baixar">⬇</a>
        <button onclick="event.stopPropagation();projRename('${escq(f.rel)}','${escq(f.name)}')" title="Renomear">✎</button>
        <button onclick="event.stopPropagation();projDelete('${escq(f.rel)}','${escq(f.name)}',false)" title="Excluir">🗑</button>
      </div>
    </div>`;}).join("");

  const vazio=(!folders && !files) ? `<div class="dash-empty"><b>Pasta vazia</b>Faça upload, crie uma pasta, ou arraste arquivos aqui.</div>` : "";
  box.innerHTML=projHeader()+`
    <div class="proj-bar">
      ${crumbs}
      <div class="proj-tools">
        <button class="kbtn" onclick="projMkdir()">📁 Nova pasta</button>
        <label class="kbtn" style="cursor:pointer">📄 Enviar arquivos
          <input type="file" id="projFile" multiple accept=".stl,.3mf,.obj,.gcode,.g,.gco" style="display:none" onchange="projUpload(event)">
        </label>
        <label class="kbtn kbtn-primary" style="cursor:pointer">📂 Enviar pasta
          <input type="file" id="projFolder" webkitdirectory directory multiple style="display:none" onchange="projUpload(event)">
        </label>
      </div>
    </div>
    <div class="proj-progress" id="projProgress" style="display:none"></div>
    <div class="proj-grid" id="projGrid">${folders}${files}${vazio}</div>
    <div class="proj-drop" id="projDrop"><div class="pd-inner">📥<br>Solte para enviar</div></div>`;
  setupProjDnd();
}
function projHeader(){
  return `<div class="proj-locals">
    <button class="proj-tab ${projLocal==='local'?'active':''}" onclick="projSwitch('local')">💻 Neste computador</button>
    <button class="proj-tab ${projLocal==='nuvem'?'active':''}" onclick="projSwitch('nuvem')">☁️ Nuvem</button>
  </div>`;
}
function projBreadcrumb(){
  const parts=projPath?projPath.split("/"):[];
  let acc="", html=`<button class="crumb" onclick="projNav('')"
    ondragover="projCrumbOver(event)" ondragleave="projCrumbLeave(event)"
    ondrop="projFolderDrop(event,'')">🏠 Início</button>`;
  parts.forEach((p,i)=>{
    acc=acc?acc+"/"+p:p;
    html+=`<span class="crumb-sep">›</span><button class="crumb" onclick="projNav('${escq(acc)}')"
      ondragover="projCrumbOver(event)" ondragleave="projCrumbLeave(event)"
      ondrop="projFolderDrop(event,'${escq(acc)}')">${p}</button>`;
  });
  return `<div class="proj-crumbs">${html}</div>`;
}
function projIcon(ext){
  return {stl:"🧊",obj:"🧊","3mf":"📦",gcode:"⚙",g:"⚙",gco:"⚙"}[ext]||"📄";
}
function escq(s){ return (s||"").replace(/'/g,"\\'").replace(/"/g,"&quot;"); }
function projSwitch(local){
  if(local==="nuvem" && !projCloudOk){ projLocal="nuvem"; projPath=""; loadProjects(); return; }
  projLocal=local; projPath=""; loadProjects();
}
function projNav(rel){ projPath=rel; loadProjects(); }
function projOpen(rel){ projPath=rel; loadProjects(); }

/* ── Reorganizar: arrastar itens para dentro de pastas ── */
let projDragRel=null;
function projDragStart(ev,rel){
  projDragRel=rel;
  ev.dataTransfer.effectAllowed="move";
  // marca interno para distinguir de arquivos vindos de fora (Windows)
  try{ ev.dataTransfer.setData("application/x-proj-move", rel); }catch(_){}
  ev.currentTarget.classList.add("dragging");
}
function projDragEnd(ev){
  projDragRel=null;
  ev.currentTarget.classList.remove("dragging");
  document.querySelectorAll(".proj-folder.drop-hover,.crumb.drop-hover")
    .forEach(el=>el.classList.remove("drop-hover"));
}
function projFolderOver(ev){
  if(projDragRel===null) return;             // só realça em arraste interno
  const alvo=ev.currentTarget.dataset.rel;
  if(alvo===projDragRel) return;             // não sobre si mesma
  ev.preventDefault();
  ev.dataTransfer.dropEffect="move";
  ev.currentTarget.classList.add("drop-hover");
}
function projFolderLeave(ev){
  ev.currentTarget.classList.remove("drop-hover");
}
function projCrumbOver(ev){
  if(projDragRel===null) return;
  ev.preventDefault();
  ev.dataTransfer.dropEffect="move";
  ev.currentTarget.classList.add("drop-hover");
}
function projCrumbLeave(ev){
  ev.currentTarget.classList.remove("drop-hover");
}
async function projFolderDrop(ev,destino){
  // se não for arraste interno, deixa o handler de upload (de fora) cuidar
  if(projDragRel===null) return;
  ev.preventDefault(); ev.stopPropagation();
  ev.currentTarget.classList.remove("drop-hover");
  const origem=projDragRel;
  projDragRel=null;
  if(origem===destino) return;
  try{
    const r=await fetch("/api/projetos/mover",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({local:projLocal, origem, destino})});
    const d=await r.json();
    if(!d.ok){ alert(d.error||"Não foi possível mover."); return; }
    loadProjects();
  }catch(_){ alert("Erro ao mover."); }
}
async function saveCloudDir(){
  const v=document.getElementById("cloudDirInput").value.trim();
  if(!v){ alert("Cole o caminho da pasta."); return; }
  try{
    const r=await fetch("/api/projetos/config",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({cloud_dir:v})});
    const d=await r.json();
    if(!d.ok){ alert(d.error||"Erro."); return; }
    projCloudOk=true; projLocal="nuvem"; projPath=""; loadProjects();
  }catch(_){ alert("Erro ao salvar."); }
}
async function projMkdir(){
  const nome=prompt("Nome da nova pasta:");
  if(!nome) return;
  const r=await fetch("/api/projetos/mkdir",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({local:projLocal,path:projPath,nome})});
  const d=await r.json();
  if(!d.ok){ alert(d.error||"Erro."); return; }
  loadProjects();
}
const PROJ_ACCEPT=[".stl",".3mf",".obj",".gcode",".g",".gco"];
function projExtOk(nome){
  const n=(nome||"").toLowerCase();
  return PROJ_ACCEPT.some(e=>n.endsWith(e));
}
async function projUpload(ev){
  // arquivos do <input>. Para upload de pasta, webkitRelativePath traz o caminho.
  const items=[...ev.target.files].map(f=>({
    file:f,
    rel:(f.webkitRelativePath||f.name)
  }));
  ev.target.value="";           // permite reenviar o mesmo arquivo depois
  await projSendFiles(items);
}
async function projSendFiles(items){
  // filtra só os tipos aceitos
  const validos=items.filter(it=>projExtOk(it.rel));
  const ignorados=items.length-validos.length;
  if(!validos.length){
    if(items.length) alert("Nenhum arquivo compatível. Aceitos: STL, 3MF, OBJ, G-code.");
    return;
  }
  const prog=document.getElementById("projProgress");
  prog.style.display="block";
  let done=0, erros=0;
  for(const it of validos){
    prog.textContent=`Enviando ${it.rel} (${done+1}/${validos.length})…`;
    // separa o subcaminho (pastas) do nome do arquivo
    const parts=it.rel.split("/");
    parts.pop();                                   // remove o nome do arquivo
    const sub=parts.join("/");
    const dest=projPath ? (sub?projPath+"/"+sub:projPath) : sub;
    const fd=new FormData();
    fd.append("file",it.file); fd.append("local",projLocal); fd.append("path",dest);
    try{
      const r=await fetch("/api/projetos/upload",{method:"POST",body:fd});
      const d=await r.json();
      if(!d.ok){ erros++; console.warn(it.rel, d.error); }
    }catch(_){ erros++; }
    done++;
  }
  prog.style.display="none";
  if(erros) alert(`${erros} arquivo(s) não puderam ser enviados.`);
  else if(ignorados) alert(`Enviados ${validos.length}. ${ignorados} arquivo(s) ignorado(s) por tipo não compatível.`);
  loadProjects();
}

/* ── Arrastar e soltar ────────────────────────────────── */
function setupProjDnd(){
  const box=document.getElementById("projectsContent");
  if(!box || box._dndReady) return;
  box._dndReady=true;
  let depth=0;
  // busca o overlay na hora (o innerHTML é recriado a cada loadProjects)
  const getDrop=()=>document.getElementById("projDrop");
  box.addEventListener("dragenter",e=>{
    if(!e.dataTransfer || ![...e.dataTransfer.types].includes("Files")) return;
    e.preventDefault(); depth++; const d=getDrop(); if(d) d.classList.add("show");
  });
  box.addEventListener("dragover",e=>{
    if(e.dataTransfer && [...e.dataTransfer.types].includes("Files")) e.preventDefault();
  });
  box.addEventListener("dragleave",e=>{
    depth--; if(depth<=0){ depth=0; const d=getDrop(); if(d) d.classList.remove("show"); }
  });
  box.addEventListener("drop",async e=>{
    // ignora se for arraste interno (mover item) — tratado nas pastas
    if(projDragRel!==null) return;
    e.preventDefault(); depth=0; const d=getDrop(); if(d) d.classList.remove("show");
    const dt=e.dataTransfer;
    if(!dt) return;
    // tenta ler a estrutura de pastas (quando arrastam uma pasta)
    const items=dt.items ? [...dt.items] : [];
    const entries=items.map(it=>it.webkitGetAsEntry && it.webkitGetAsEntry()).filter(Boolean);
    if(entries.length && entries.some(en=>en.isDirectory)){
      const collected=[];
      for(const en of entries) await walkEntry(en,"",collected);
      await projSendFiles(collected);
    }else{
      // arquivos soltos direto
      const files=[...dt.files].map(f=>({file:f, rel:f.name}));
      await projSendFiles(files);
    }
  });
}
function walkEntry(entry, prefix, out){
  return new Promise(resolve=>{
    if(entry.isFile){
      entry.file(f=>{ out.push({file:f, rel:prefix+entry.name}); resolve(); },
                 ()=>resolve());
    }else if(entry.isDirectory){
      const reader=entry.createReader();
      const readAll=()=>reader.readEntries(async ents=>{
        if(!ents.length){ resolve(); return; }
        for(const en of ents) await walkEntry(en, prefix+entry.name+"/", out);
        readAll();   // continua lendo (readEntries retorna em lotes)
      }, ()=>resolve());
      readAll();
    }else resolve();
  });
}
async function projRename(rel,nome){
  const novo=prompt("Novo nome:",nome);
  if(!novo||novo===nome) return;
  const r=await fetch("/api/projetos/rename",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({local:projLocal,path:rel,nome:novo})});
  const d=await r.json();
  if(!d.ok){ alert(d.error||"Erro."); return; }
  loadProjects();
}
async function projDelete(rel,nome,isFolder){
  if(!confirm(`Excluir ${isFolder?'a pasta':'o arquivo'} "${nome}"?${isFolder?' Todo o conteúdo será perdido.':''}`)) return;
  const r=await fetch("/api/projetos/delete",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({local:projLocal,path:rel})});
  const d=await r.json();
  if(!d.ok){ alert(d.error||"Erro."); return; }
  loadProjects();
}

/* ── Abrir arquivo no fatiador ────────────────────────── */
let projAbrirCtx=null;
async function projAbrir(rel,nome){
  projAbrirCtx={rel,nome};
  let fatiadores=[];
  try{
    const d=await (await fetch("/api/projetos/fatiadores")).json();
    if(d.ok) fatiadores=d.fatiadores;
  }catch(_){}
  const modal=document.getElementById("abrirModal");
  const icone=b=>b==='bambu'?'🎋':'🅰️';
  const lista=fatiadores.map(fa=>`
    <button class="abrir-imp" onclick="projAbrirNa('${escq(fa.brand)}')">
      <div class="ai-ic">${icone(fa.brand)}</div>
      <div class="ai-tx">
        <b>${fa.nome}</b>
        <small>${fa.instalado?'Instalado':'<span style="color:#e0a94f">não encontrado — informe o caminho</span>'}</small>
      </div>
      <div class="ai-arrow">›</div>
    </button>`).join("");
  modal.innerHTML=`<div class="calc-head"><b>Abrir no fatiador</b>
    <div class="mclose" onclick="fecharAbrir()">✕</div></div>
    <div class="abrir-body">
      <div class="abrir-file">📄 ${nome}</div>
      <p class="abrir-q">Em qual programa deseja abrir?</p>
      ${lista}
      <div class="abrir-hint">O arquivo abre no programa escolhido, onde você
        ajusta o fatiamento e envia para impressão.</div>
    </div>`;
  document.getElementById("abrirOverlay").classList.add("open");
}
function fecharAbrir(){
  document.getElementById("abrirOverlay").classList.remove("open");
  projAbrirCtx=null;
}
async function projAbrirNa(brand){
  if(!projAbrirCtx) return;
  const body={local:projLocal, path:projAbrirCtx.rel, brand};
  const modal=document.getElementById("abrirModal");
  modal.querySelector(".abrir-body").innerHTML=`<div class="dash-empty"><b>Abrindo…</b>Aguarde o programa iniciar.</div>`;
  try{
    const r=await fetch("/api/projetos/abrir",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(d.ok){ fecharAbrir(); return; }
    if(d.precisa_caminho){
      modal.querySelector(".abrir-body").innerHTML=`
        <div class="abrir-file">⚠️ ${d.slicer} não encontrado</div>
        <p class="abrir-q">Informe onde o programa está instalado (arquivo .exe):</p>
        <input id="slicerPathInput" class="abrir-input" placeholder="C:\\Program Files\\...\\programa.exe">
        <div class="abrir-hint">Dica: clique com o botão direito no atalho do programa →
          Propriedades → copie o campo "Destino" (sem as aspas).</div>
        <div class="ca-btns" style="margin-top:1rem">
          <button class="ca-no" onclick="fecharAbrir()">Cancelar</button>
          <button class="ca-yes" onclick="salvarSlicerPath('${escq(d.brand)}')">Salvar e abrir</button>
        </div>`;
      return;
    }
    alert(d.error||"Não foi possível abrir.");
    fecharAbrir();
  }catch(_){ alert("Erro de conexão."); fecharAbrir(); }
}
async function salvarSlicerPath(brand){
  const caminho=document.getElementById("slicerPathInput").value.trim();
  if(!caminho){ alert("Cole o caminho do programa."); return; }
  try{
    const r=await fetch("/api/projetos/slicer_path",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify({brand,path:caminho})});
    const d=await r.json();
    if(!d.ok){ alert(d.error||"Erro."); return; }
    // tenta abrir direto no fatiador recém-configurado
    projAbrirNa(brand);
  }catch(_){ alert("Erro ao salvar."); }
}

/* ── Montagem das páginas de Relatórios e Calculadora ─── */
function mountReports(){
  const host=document.getElementById("reportsContent");
  const modal=document.getElementById("reportModal");
  if(host && modal && modal.parentElement!==host){
    host.appendChild(modal);
    modal.classList.add("embedded");
    modal.style.width="100%"; modal.style.maxWidth="100%";
  }
  const ov=document.getElementById("reportOverlay");
  if(ov) ov.classList.remove("open");
  openReports();
}
function mountCalc(){
  const host=document.getElementById("calcContent");
  const modal=document.getElementById("calcModal");
  if(host && modal && modal.parentElement!==host){
    host.appendChild(modal);
    modal.classList.add("embedded");
    modal.style.width="100%"; modal.style.maxWidth="100%";
  }
  const ov=document.getElementById("calcOverlay");
  if(ov) ov.classList.remove("open");
  openCalc();
}

/* ── Custo por impressão ──────────────────────────────── */
let printCosts={};          // nome -> {file, custo, material, peso_g, skip}
let custoPerguntando=null;  // impressora sendo perguntada agora

function jobKey(st){
  const p=st.print||{};
  return ((p.subtask_name||p.gcode_file||"")+"").split("/").pop()||"(sem nome)";
}
function precisaPerguntar(name,st){
  const meta=st._meta||{}, p=st.print||{};
  if(!meta.online) return false;
  if(p.gcode_state!=="RUNNING") return false;
  const key=jobKey(st);
  const reg=printCosts[name];
  if(reg && reg.file===key) return false;   // já respondeu para ESTA impressão
  return true;
}
function checkNovaImpressao(){
  if(custoPerguntando) return;
  const ov=document.getElementById("custoOverlay");
  if(!ov || ov.classList.contains("open")) return;
  for(const [name,st] of Object.entries(lastData||{})){
    if(precisaPerguntar(name,st)){ perguntarCusto(name,st); return; }
  }
}
function perguntarCusto(name,st){
  custoPerguntando=name;
  const meta=st._meta||{};
  const disp=(meta.apelido||"").trim()||name;
  document.getElementById("custoModal").innerHTML=`
    <div class="calc-head"><b>🖨️ Nova impressão detectada</b></div>
    <div class="custo-ask">
      <div class="ca-printer">${disp}</div>
      <div class="ca-file">${jobKey(st)}</div>
      <p class="ca-q">Deseja inserir o custo desta impressão?</p>
      <div class="ca-btns">
        <button class="ca-no" onclick="pularCusto()">Não</button>
        <button class="ca-yes" onclick="formCusto()">Sim, informar custo</button>
      </div>
      <div class="ca-hint">Se escolher "Não", o monitoramento segue normalmente
        e esta impressão fica sem custo no relatório.</div>
    </div>`;
  document.getElementById("custoOverlay").classList.add("open");
}
function fecharCusto(){
  document.getElementById("custoOverlay").classList.remove("open");
  custoPerguntando=null;
}
async function pularCusto(){
  const name=custoPerguntando;
  const st=lastData[name]||{};
  const key=jobKey(st);
  fecharCusto();
  try{
    await fetch("/api/custo/skip",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({name, file:key})});
  }catch(_){}
  printCosts[name]={file:key, skip:true};
}
async function formCusto(){
  const name=custoPerguntando;
  const st=lastData[name]||{};
  const meta=st._meta||{}, p=st.print||{};
  const disp=(meta.apelido||"").trim()||name;

  // tempo estimado — o sistema já sabe pela impressora
  const pct=Math.max(0,Math.min(100,Math.round(p.mc_percent??0)));
  const remain=p.mc_remaining_time;
  let minutos=0;
  if(pct>0&&pct<100&&remain!=null) minutos=Math.round(remain/(1-pct/100));
  else if(remain!=null) minutos=Math.round(remain);

  let cfg={preco_kg:120,preco_kwh:0.95};
  try{
    const r=await fetch("/api/calc/config");
    const d=await r.json();
    if(d.ok&&d.cfg) cfg=d.cfg;
  }catch(_){}

  document.getElementById("custoModal").innerHTML=`
    <div class="calc-head"><b>💰 Custo da impressão</b>
      <div class="mclose" onclick="fecharCusto()">✕</div></div>
    <div class="custo-form">
      <div class="ca-printer" style="margin-bottom:.15rem">${disp}</div>
      <div class="ca-file" style="margin-bottom:1rem">${jobKey(st)}</div>
      <div class="cf-row"><label>Material</label>
        <select id="cf_material">
          <option>PLA</option><option>PETG</option>
          <option>ABS</option><option>TPU</option>
        </select></div>
      <div class="cf-row"><label>Preço do filamento</label>
        <input type="number" id="cf_preco_kg" value="${cfg.preco_kg}" step="1" min="0"
          oninput="custoPreview()"><span class="cf-u">R$/kg</span></div>
      <div class="cf-row"><label>Peso da peça</label>
        <input type="number" id="cf_peso" value="" step="1" min="0" placeholder="ex: 45"
          oninput="custoPreview()"><span class="cf-u">g</span></div>
      <div class="cf-row"><label>Tempo de impressão</label>
        <input type="number" id="cf_min" value="${minutos}" step="1" min="0"
          oninput="custoPreview()"><span class="cf-u">min</span></div>
      <div class="cf-row"><label>Custo da energia</label>
        <input type="number" id="cf_kwh" value="${cfg.preco_kwh}" step="0.01" min="0"
          oninput="custoPreview()"><span class="cf-u">R$/kWh</span></div>
      <div class="cf-out" id="cf_out">Informe o peso para calcular.</div>
      <div class="ca-hint" style="margin-top:.6rem">O peso da peça aparece no seu
        fatiador ao fatiar o modelo. O tempo já veio da impressora — pode ajustar.</div>
      <div class="ca-btns" style="margin-top:1rem">
        <button class="ca-no" onclick="pularCusto()">Pular</button>
        <button class="ca-yes" onclick="salvarCusto()">Salvar custo</button>
      </div>
    </div>`;
  custoPreview();
}
function custoPreview(){
  const g=id=>parseFloat((document.getElementById(id)||{}).value)||0;
  const peso=g("cf_peso"), pkg=g("cf_preco_kg"), min=g("cf_min"), kwh=g("cf_kwh");
  const out=document.getElementById("cf_out");
  if(!out) return;
  if(peso<=0){ out.innerHTML="Informe o peso para calcular."; return; }
  const material=(peso/1000)*pkg;
  const energia=(150/1000)*(min/60)*kwh;
  out.innerHTML=`<div class="cf-l"><span>Material</span><b>${money(material)}</b></div>
    <div class="cf-l"><span>Energia</span><b>${money(energia)}</b></div>
    <div class="cf-t"><span>Custo desta impressão</span><b>${money(material+energia)}</b></div>`;
}
async function salvarCusto(){
  const name=custoPerguntando;
  const st=lastData[name]||{};
  const g=id=>parseFloat((document.getElementById(id)||{}).value)||0;
  const peso=g("cf_peso");
  if(peso<=0){ alert("Informe o peso da peça em gramas."); return; }
  const body={name, file:jobKey(st),
    material:document.getElementById("cf_material").value,
    preco_kg:g("cf_preco_kg"), peso_g:peso,
    minutos:g("cf_min"), preco_kwh:g("cf_kwh")};
  try{
    const r=await fetch("/api/custo/set",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(!d.ok){ alert(d.error||"Não foi possível salvar."); return; }
    printCosts[name]={file:body.file, custo:d.custo, material:body.material,
      peso_g:peso, skip:false};
    fecharCusto();
    if(lastData) render(lastData, lastOrder);
  }catch(_){ alert("Erro ao salvar o custo."); }
}

/* ── Calculadora de custo ─────────────────────────────── */
let calcCfg={preco_kg:120,potencia_w:150,preco_kwh:0.95,valor_maquina:3000,
  vida_util_h:5000,margem_pct:100,falha_pct:5};
let calcPeso=50, calcHoras=3, calcQtd=1;

async function openCalc(){
  try{
    const r=await fetch("/api/calc/config");
    const d=await r.json();
    if(d.ok&&d.cfg) calcCfg=Object.assign(calcCfg,d.cfg);
  }catch(_){}
  renderCalc();
  // só abre como overlay se o modal ainda estiver no overlay (fora da página)
  const modal=document.getElementById("calcModal");
  if(modal && modal.closest("#calcOverlay")){
    document.getElementById("calcOverlay").classList.add("open");
  }
}
function closeCalc(){
  document.getElementById("calcOverlay").classList.remove("open");
}
function money(v){
  return "R$ "+(v||0).toLocaleString("pt-BR",{minimumFractionDigits:2,maximumFractionDigits:2});
}
function calcRow(label,id,val,unit,step){
  return `<div class="calc-row"><label>${label}</label>
    <input type="number" id="${id}" value="${val}" step="${step||'0.01'}" min="0"
      oninput="calcCompute()"><span class="unit">${unit||''}</span></div>`;
}
function renderCalc(){
  // impressoras imprimindo agora (para o botão de puxar dados)
  const rodando=Object.entries(lastData||{}).filter(([n,st])=>{
    const p=st.print||{}; return (st._meta||{}).online && p.gcode_state==="RUNNING";
  });
  const pullBtn=rodando.length
    ? `<button class="calc-pull" onclick="calcPull()">⬇ Puxar tempo da impressão em andamento (${rodando.length})</button>`
    : `<div class="calc-note" style="margin:0 0 .9rem">Nenhuma impressão em andamento para puxar o tempo automaticamente. Preencha manualmente abaixo.</div>`;

  document.getElementById("calcModal").innerHTML=`
    <div class="calc-head"><b>🧮 Calculadora de custo</b></div>
    <div class="calc-body">
      ${pullBtn}
      <div class="calc-grid">
        <div>
          <div class="calc-sec">
            <h4>Esta peça</h4>
            ${calcRow("Peso da peça","c_peso",calcPeso,"g","1")}
            ${calcRow("Tempo de impressão","c_horas",calcHoras,"h","0.1")}
            ${calcRow("Quantidade","c_qtd",calcQtd,"un","1")}
          </div>
          <div class="calc-sec">
            <h4>Material</h4>
            ${calcRow("Preço do filamento","c_preco_kg",calcCfg.preco_kg,"R$/kg","1")}
          </div>
          <div class="calc-sec">
            <h4>Energia</h4>
            ${calcRow("Consumo da impressora","c_potencia_w",calcCfg.potencia_w,"W","10")}
            ${calcRow("Preço da energia","c_preco_kwh",calcCfg.preco_kwh,"R$/kWh","0.01")}
          </div>
          <div class="calc-sec">
            <h4>Máquina e margem</h4>
            ${calcRow("Valor da impressora","c_valor_maquina",calcCfg.valor_maquina,"R$","100")}
            ${calcRow("Vida útil estimada","c_vida_util_h",calcCfg.vida_util_h,"h","500")}
            ${calcRow("Perda por falhas","c_falha_pct",calcCfg.falha_pct,"%","1")}
            ${calcRow("Margem de lucro","c_margem_pct",calcCfg.margem_pct,"%","5")}
          </div>
          <button class="calc-save" onclick="calcSave()">💾 Salvar meus preços</button>
        </div>
        <div>
          <div class="calc-out" id="calcOut"></div>
          <div class="calc-note">
            <b>Como é calculado:</b><br>
            • <b>Material</b> = peso × preço do filamento<br>
            • <b>Energia</b> = consumo × tempo × preço do kWh<br>
            • <b>Máquina</b> = depreciação (valor ÷ vida útil) × tempo<br>
            • <b>Falhas</b> = percentual sobre o custo, para cobrir impressões perdidas<br>
            • <b>Preço sugerido</b> = custo + margem de lucro<br><br>
            O peso da peça você encontra no seu fatiador (Bambu Studio, Orca,
            Anycubic Slicer) ao fatiar o modelo.
          </div>
        </div>
      </div>
    </div>`;
  calcCompute();
}
function calcNum(id,fallback){
  const el=document.getElementById(id);
  if(!el) return fallback;
  const v=parseFloat(el.value);
  return isNaN(v)?0:v;
}
function calcCompute(){
  const peso=calcNum("c_peso",0), horas=calcNum("c_horas",0), qtd=Math.max(1,calcNum("c_qtd",1));
  const precoKg=calcNum("c_preco_kg",0), potW=calcNum("c_potencia_w",0);
  const precoKwh=calcNum("c_preco_kwh",0), valorMaq=calcNum("c_valor_maquina",0);
  const vidaH=calcNum("c_vida_util_h",0), falhaPct=calcNum("c_falha_pct",0);
  const margemPct=calcNum("c_margem_pct",0);

  const material=(peso/1000)*precoKg;
  const energia=(potW/1000)*horas*precoKwh;
  const maquina=vidaH>0?(valorMaq/vidaH)*horas:0;
  const subtotal=material+energia+maquina;
  const falhas=subtotal*(falhaPct/100);
  const custoUnit=subtotal+falhas;
  const custoTotal=custoUnit*qtd;
  const precoUnit=custoUnit*(1+margemPct/100);
  const precoTotal=precoUnit*qtd;
  const lucro=precoTotal-custoTotal;

  const out=document.getElementById("calcOut");
  if(!out) return;
  out.innerHTML=`
    <div class="calc-line"><span>Material (${peso.toFixed(0)}g)</span><b>${money(material)}</b></div>
    <div class="calc-line"><span>Energia (${horas.toFixed(1)}h)</span><b>${money(energia)}</b></div>
    <div class="calc-line"><span>Máquina (depreciação)</span><b>${money(maquina)}</b></div>
    <div class="calc-line"><span>Reserva p/ falhas (${falhaPct.toFixed(0)}%)</span><b>${money(falhas)}</b></div>
    <div class="calc-line"><span>Custo por peça</span><b>${money(custoUnit)}</b></div>
    <div class="calc-total"><span>Custo total (${qtd}un)</span><b>${money(custoTotal)}</b></div>
    <div class="calc-sell"><span>Preço de venda (${qtd}un)</span><b>${money(precoTotal)}</b></div>
    <div class="calc-line" style="margin-top:.7rem"><span>Lucro estimado</span><b style="color:#35d17c">${money(lucro)}</b></div>
    <div class="calc-line"><span>Preço unitário</span><b>${money(precoUnit)}</b></div>`;
}
function calcPull(){
  // pega a impressão em andamento com maior tempo estimado
  const rodando=Object.entries(lastData||{}).filter(([n,st])=>{
    const p=st.print||{}; return (st._meta||{}).online && p.gcode_state==="RUNNING";
  });
  if(!rodando.length){ alert("Nenhuma impressão em andamento."); return; }
  let melhorH=null, qualNome="";
  for(const [n,st] of rodando){
    const p=st.print||{};
    const pct=Math.max(0,Math.min(100,Math.round(p.mc_percent??0)));
    const remain=p.mc_remaining_time;   // minutos
    if(pct>0&&pct<100&&remain!=null){
      const totalMin=remain/(1-pct/100);
      const h=totalMin/60;
      if(melhorH===null||h>melhorH){ melhorH=h; qualNome=((st._meta||{}).apelido||n); }
    }
  }
  if(melhorH===null){
    alert("Ainda não dá para estimar o tempo total (a impressão precisa ter avançado um pouco).");
    return;
  }
  const el=document.getElementById("c_horas");
  if(el){ el.value=melhorH.toFixed(1); }
  calcCompute();
  alert(`Tempo puxado de "${qualNome}": ${melhorH.toFixed(1)}h\n\nO peso da peça precisa ser informado manualmente — as impressoras não reportam esse dado.`);
}
async function calcSave(){
  const body={
    preco_kg:calcNum("c_preco_kg",0), potencia_w:calcNum("c_potencia_w",0),
    preco_kwh:calcNum("c_preco_kwh",0), valor_maquina:calcNum("c_valor_maquina",0),
    vida_util_h:calcNum("c_vida_util_h",0), margem_pct:calcNum("c_margem_pct",0),
    falha_pct:calcNum("c_falha_pct",0)};
  try{
    const r=await fetch("/api/calc/config",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(d.ok){ calcCfg=Object.assign(calcCfg,d.cfg); alert("Preços salvos! Serão lembrados na próxima vez."); }
    else alert(d.error||"Não foi possível salvar.");
  }catch(_){ alert("Erro ao salvar."); }
}

function openLogoPicker(){
  document.getElementById("logoFileInput").click();
}

/* ── Contato via WhatsApp ─────────────────────────────── */
const WHATSAPP_NUM="5512988447240";   // (12) 98844-7240
async function abrirWhatsapp(tipo){
  let msg;
  if(tipo==="licenca"){
    // já manda o Código da Máquina junto — poupa uma ida e volta
    let fp="";
    try{
      const r=await fetch("/api/licenca/info");
      const d=await r.json();
      fp=d.fingerprint||"";
    }catch(_){}
    msg=`Olá! Gostaria de comprar a licença do 3DWORK FARM.`
      + (fp?`\n\nCódigo da Máquina: ${fp}`:"");
  }else{
    msg="Olá! Preciso de suporte com o 3DWORK FARM.";
  }
  const url=`https://wa.me/${WHATSAPP_NUM}?text=${encodeURIComponent(msg)}`;
  window.open(url,"_blank");
  closeMenu();
}
function handleLogoFile(ev){
  const file=ev.target.files[0];
  if(!file) return;
  if(file.size>5*1024*1024){ alert("Imagem muito grande (máx. 5 MB)."); return; }
  const reader=new FileReader();
  reader.onload=async(e)=>{
    const img=new Image();
    img.onload=async()=>{
      // Normaliza qualquer logo para uma caixa de proporção fixa, preservando
      // o aspecto original (sem distorcer) e centralizando. Assim, logos
      // largos, altos ou quadrados sempre ficam bem na barra lateral.
      const BOX_W=360, BOX_H=150;     // proporção ~2.4:1, boa para a sidebar
      const PAD=10;                    // respiro nas bordas
      const availW=BOX_W-PAD*2, availH=BOX_H-PAD*2;
      // escala para caber DENTRO da caixa mantendo a proporção
      const scale=Math.min(availW/img.width, availH/img.height);
      const dw=Math.round(img.width*scale), dh=Math.round(img.height*scale);
      const dx=Math.round((BOX_W-dw)/2), dy=Math.round((BOX_H-dh)/2);
      const canvas=document.createElement("canvas");
      canvas.width=BOX_W; canvas.height=BOX_H;
      const ctx=canvas.getContext("2d");
      // fundo transparente + suavização de qualidade
      ctx.clearRect(0,0,BOX_W,BOX_H);
      ctx.imageSmoothingEnabled=true;
      ctx.imageSmoothingQuality="high";
      ctx.drawImage(img,0,0,img.width,img.height,dx,dy,dw,dh);
      const dataUri=canvas.toDataURL("image/png");
      try{
        const r=await fetch("/api/logo",{method:"POST",
          headers:{"Content-Type":"application/json"},
          body:JSON.stringify({data:dataUri})});
        const d=await r.json();
        if(d.ok){
          document.querySelectorAll(".sb-logo, .brand .logo, img.logo").forEach(el=>el.src=dataUri);
          alert("Logo atualizado com sucesso!");
          closeMenu();
        }else{
          alert(d.msg||"Erro ao enviar o logo.");
        }
      }catch(_){ alert("Erro ao enviar o logo."); }
    };
    img.onerror=()=>alert("Não foi possível ler essa imagem. Use um arquivo PNG ou JPG válido.");
    img.src=e.target.result;
  };
  reader.readAsDataURL(file);
}

/* ── Modos de visualização dos cards ──────────────────── */
const VIEW_MODES=[
  {id:"full",    label:"▦ Completo"},
  {id:"compact", label:"▤ Compacto"},
  {id:"list",    label:"☰ Lista"},
  {id:"focus",   label:"◉ Foco"},
  {id:"mosaic",  label:"▪ Mosaico"},
];
let viewModeIdx=0;
try{
  const saved=localStorage.getItem("hub3d_viewmode");
  if(saved){ const i=VIEW_MODES.findIndex(m=>m.id===saved); if(i>=0) viewModeIdx=i; }
}catch(_){}

function applyViewMode(){
  const m=VIEW_MODES[viewModeIdx];
  const main=document.getElementById("grid");
  VIEW_MODES.forEach(v=>main.classList.remove("view-"+v.id));
  main.classList.add("view-"+m.id);
  const btn=document.getElementById("viewModeBtn");
  if(btn) btn.textContent=m.label;
  try{ localStorage.setItem("hub3d_viewmode", m.id); }catch(_){}
  applyCols();
}
function cycleViewMode(){
  viewModeIdx=(viewModeIdx+1)%VIEW_MODES.length;
  applyViewMode();
}

/* ── Colunas por linha (0 = automático) ───────────────── */
const COLS_OPTIONS=[0,1,2,3,4,5,6];
let colsIdx=0;
try{
  const saved=localStorage.getItem("hub3d_cols");
  if(saved!==null){ const n=parseInt(saved,10); const i=COLS_OPTIONS.indexOf(n); if(i>=0) colsIdx=i; }
}catch(_){}

function applyCols(){
  const n=COLS_OPTIONS[colsIdx];
  const main=document.getElementById("grid");
  const btn=document.getElementById("colsBtn");
  if(n===0){
    main.style.removeProperty("--cols-override");
    main.classList.remove("cols-fixed");
    if(btn) btn.textContent="⊞ Auto";
  }else{
    main.style.setProperty("--cols-override", n);
    main.classList.add("cols-fixed");
    if(btn) btn.textContent=`⊞ ${n} ${n===1?'coluna':'colunas'}`;
  }
  try{ localStorage.setItem("hub3d_cols", String(n)); }catch(_){}
}
function cycleCols(){
  colsIdx=(colsIdx+1)%COLS_OPTIONS.length;
  applyCols();
}

/* ── Relatórios ───────────────────────────────────────── */
let reportState={period:"mes", selected:new Set(), data:null};

function openReports(){
  const modal=document.getElementById("reportModal");
  if(modal && modal.closest("#reportOverlay")){
    document.getElementById("reportOverlay").classList.add("open");
  }
  loadReport();
}
function closeReports(){
  document.getElementById("reportOverlay").classList.remove("open");
}
function setPeriod(p){ reportState.period=p; loadReport(); }
function togglePrinterFilter(name){
  if(reportState.selected.has(name)) reportState.selected.delete(name);
  else reportState.selected.add(name);
  loadReport();
}

async function loadReport(){
  const modal=document.getElementById("reportModal");
  const sel=[...reportState.selected];
  const q=`period=${reportState.period}`+(sel.length?`&printers=${encodeURIComponent(sel.join(","))}`:"");
  modal.innerHTML=`<div class="rep-head"><b>📊 Relatórios de Impressão</b></div><div class="rep-loading">Carregando…</div>`;
  try{
    const r=await fetch(`/api/report?${q}`);
    const d=await r.json();
    reportState.data=d;
    renderReport(d);
  }catch(_){
    modal.innerHTML=`<div class="rep-head"><b>📊 Relatórios</b></div><div class="rep-loading">Erro ao carregar.</div>`;
  }
}

function renderReport(d){
  const modal=document.getElementById("reportModal");
  const periods=[["dia","Dia"],["semana","Semana"],["mes","Mês"],["ano","Ano"],["tudo","Tudo"]];
  const periodBtns=periods.map(([id,lb])=>
    `<button class="rep-pbtn ${reportState.period===id?'on':''}" onclick="setPeriod('${id}')">${lb}</button>`).join("");

  const allPrinters=d.available_printers||[];
  const chips=allPrinters.map(p=>{
    const on=reportState.selected.size===0||reportState.selected.has(p);
    return `<button class="rep-chip ${reportState.selected.has(p)?'on':''}" onclick="togglePrinterFilter('${p.replace(/'/g,"\\'")}')">${p}</button>`;
  }).join("");

  const rate=d.success_rate!=null?d.success_rate:0;
  // Donut de sucesso/falha
  const circ=2*Math.PI*52;
  const dash=circ*(rate/100);
  const donut=`
    <svg viewBox="0 0 130 130" class="rep-donut">
      <circle cx="65" cy="65" r="52" fill="none" stroke="#2a2f3e" stroke-width="16"/>
      <circle cx="65" cy="65" r="52" fill="none" stroke="#37d67a" stroke-width="16"
        stroke-dasharray="${dash} ${circ}" stroke-linecap="round" transform="rotate(-90 65 65)"/>
      <text x="65" y="60" text-anchor="middle" class="rep-donut-pct">${rate}%</text>
      <text x="65" y="80" text-anchor="middle" class="rep-donut-lbl">sucesso</text>
    </svg>`;

  // Barras por impressora
  const byP=[...(d.by_printer||[])].sort((a,b)=>b.total-a.total);
  const maxT=Math.max(1,...byP.map(p=>p.total));
  const bars=byP.map(p=>{
    const sw=p.total?Math.round(p.success/p.total*100):0;
    return `<div class="rep-bar-row">
      <div class="rep-bar-name" title="${p.printer}">${p.printer}</div>
      <div class="rep-bar-track">
        <div class="rep-bar-fill" style="width:${p.total/maxT*100}%">
          <span class="rep-bar-succ" style="width:${sw}%"></span>
        </div>
      </div>
      <div class="rep-bar-val">${p.total}</div>
    </div>`;
  }).join("")||`<div class="rep-empty">Sem impressões no período.</div>`;

  const fmtDur=(s)=>{ if(!s)return"—"; const h=Math.floor(s/3600),m=Math.floor((s%3600)/60); return h?`${h}h ${m}min`:`${m}min`; };

  // Tabela detalhada
  const rows=(d.jobs||[]).slice(0,60).map(j=>{
    const fin=(j.finished_at||"").slice(0,16).replace("T"," ");
    const ok=j.result==="success";
    const mat=j.material?`${j.material}${j.peso_g?` ${j.peso_g.toFixed(0)}g`:""}`:"—";
    const cst=j.custo?`<b style="color:#35d17c">${money(j.custo)}</b>`:"—";
    return `<tr>
      <td>${fin}</td><td>${j.printer}</td>
      <td class="rep-file">${j.file||"—"}</td>
      <td class="rep-file">${j.config||"—"}</td>
      <td><span class="rep-res ${ok?'ok':'fail'}">${ok?'✓ Sucesso':'✗ Falha'}</span></td>
      <td>${fmtDur(j.duration_sec)}</td>
      <td>${mat}</td><td>${cst}</td>
    </tr>`;
  }).join("")||`<tr><td colspan="8" class="rep-empty">Nenhuma impressão registrada.</td></tr>`;

  const selQuery=[...reportState.selected];
  const pdfQ=`period=${reportState.period}`+(selQuery.length?`&printers=${encodeURIComponent(selQuery.join(","))}`:"");

  modal.innerHTML=`
    <div class="rep-head">
      <b>📊 Relatórios de Impressão</b>
    </div>
    <div class="rep-body">
      <div class="rep-controls">
        <div class="rep-periods">${periodBtns}</div>
        <a class="rep-pdf" href="/api/report/pdf?${pdfQ}" target="_blank">⬇ Exportar PDF</a>
      </div>
      ${allPrinters.length?`<div class="rep-filter"><span class="rep-flabel">Impressoras:</span>${chips}<button class="rep-chip-clear" onclick="reportState.selected.clear();loadReport()">Todas</button></div>`:""}
      <div class="rep-cards">
        <div class="rep-stat"><div class="rep-stat-v">${d.total}</div><div class="rep-stat-k">Impressões</div></div>
        <div class="rep-stat"><div class="rep-stat-v" style="color:#37d67a">${d.success}</div><div class="rep-stat-k">Sucesso</div></div>
        <div class="rep-stat"><div class="rep-stat-v" style="color:#ff5470">${d.failed}</div><div class="rep-stat-k">Falhas</div></div>
        <div class="rep-stat"><div class="rep-stat-v">${d.total_hours}h</div><div class="rep-stat-k">Horas</div></div>
        <div class="rep-stat"><div class="rep-stat-v">${(d.total_peso_g||0).toFixed(0)}g</div><div class="rep-stat-k">Filamento</div></div>
        <div class="rep-stat"><div class="rep-stat-v" style="color:#35d17c">${money(d.total_custo||0)}</div><div class="rep-stat-k">Custo total</div></div>
      </div>
      <div class="rep-charts">
        <div class="rep-chart-box">
          <div class="rep-chart-title">Taxa de sucesso</div>
          ${donut}
        </div>
        <div class="rep-chart-box rep-bars-box">
          <div class="rep-chart-title">Impressões por impressora</div>
          <div class="rep-bars">${bars}</div>
        </div>
      </div>
      <div class="rep-table-title">Impressões detalhadas ${d.jobs&&d.jobs.length>60?'(últimas 60)':''}</div>
      <div class="rep-table-wrap">
        <table class="rep-table">
          <thead><tr><th>Data</th><th>Impressora</th><th>Arquivo</th><th>Configuração</th><th>Resultado</th><th>Tempo</th><th>Material</th><th>Custo</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>`;
}

async function enterKiosk(){
  nav("printers");   // garante que a farm esteja visível
  document.body.classList.add("kiosk");
  try{ await document.documentElement.requestFullscreen(); }catch(_){}
  try{ if("wakeLock" in navigator) wakeLock=await navigator.wakeLock.request("screen"); }catch(_){}
}
async function exitKiosk(){
  document.body.classList.remove("kiosk");
  try{ if(document.fullscreenElement) await document.exitFullscreen(); }catch(_){}
  try{ if(wakeLock){ await wakeLock.release(); wakeLock=null; } }catch(_){}
}
document.addEventListener("fullscreenchange",()=>{ if(!document.fullscreenElement) document.body.classList.remove("kiosk"); });
document.addEventListener("visibilitychange",async()=>{
  if(document.visibilityState==="visible" && document.body.classList.contains("kiosk") && "wakeLock" in navigator){
    try{ wakeLock=await navigator.wakeLock.request("screen"); }catch(_){}
  }
});
document.addEventListener("keydown",e=>{
  if(e.key==="Escape"){
    if(document.getElementById("addOverlay").classList.contains("open")) closeAdd();
    else if(openName) closeDetail();
    else if(document.body.classList.contains("kiosk")) exitKiosk();
  }
});

let ws;
function connect(){
  ws=new WebSocket(`ws://${location.host}/ws`);
  ws.onopen=()=>{document.getElementById("dot").classList.add("live");
    document.getElementById("connlbl").textContent="ao vivo";};
  ws.onmessage=e=>{try{const m=JSON.parse(e.data);
    if(m.costs) printCosts=m.costs;
    render(m.printers||{},m.order);
    checkNovaImpressao();
    if(currentPage==="dashboard") renderDash();
  }catch(_){}}
  ws.onclose=()=>{document.getElementById("dot").classList.remove("live");
    document.getElementById("connlbl").textContent="reconectando…"; setTimeout(connect,2000);}
}
connect();
applyViewMode();
applyCols();
nav("dashboard");   // começa no Dashboard
checkUpdate();      // verifica se há versão nova (silencioso se não houver)

/* ── Atualização do sistema ───────────────────────────── */
let updateInfo=null;
async function checkUpdate(silent=true){
  try{
    const d=await (await fetch("/api/update/check")).json();
    updateInfo=d;
    if(d.ok && d.disponivel){
      mostrarAvisoUpdate(d);
    }else if(!silent){
      if(d.motivo==="nao_configurado")
        alert("A verificação de atualização ainda não foi configurada.");
      else if(d.ok && !d.disponivel)
        alert("Você já está na versão mais recente ("+d.atual+").");
      else
        alert(d.erro||"Não foi possível verificar.");
    }
  }catch(_){ if(!silent) alert("Erro ao verificar atualização."); }
}
function mostrarAvisoUpdate(d){
  // banner no topo do conteúdo, discreto mas visível
  if(document.getElementById("updateBanner")) return;
  const bar=document.createElement("div");
  bar.id="updateBanner"; bar.className="update-banner";
  bar.innerHTML=`<span>🎉 Nova versão disponível: <b>${d.nova}</b>${d.notas?` — ${d.notas}`:""}</span>
    <div class="ub-btns">
      <button class="ub-later" onclick="fecharBanner()">Depois</button>
      <button class="ub-now" onclick="aplicarUpdate()">Atualizar agora</button>
    </div>`;
  const content=document.querySelector(".content");
  const topbar=document.querySelector(".topbar");
  if(content && topbar) content.insertBefore(bar, topbar.nextSibling);
}
function fecharBanner(){
  const b=document.getElementById("updateBanner"); if(b) b.remove();
}
async function aplicarUpdate(){
  if(!confirm("Atualizar o sistema para a versão "+((updateInfo&&updateInfo.nova)||"nova")+
    "?\n\nO sistema será atualizado e você precisará reiniciá-lo em seguida.")) return;
  const b=document.getElementById("updateBanner");
  if(b) b.innerHTML=`<span>⏳ Baixando e aplicando a atualização…</span>`;
  try{
    const d=await (await fetch("/api/update/apply",{method:"POST"})).json();
    if(d.ok){
      if(b) b.innerHTML=`<span>✅ Atualizado para a versão <b>${d.nova}</b>!
        Feche o sistema e abra novamente para usar a nova versão.</span>`;
      alert("Atualização concluída!\n\nAgora FECHE o sistema (ícone na bandeja → Sair, "+
            "ou feche a janela do programa) e ABRA de novo.\n\n"+
            "Uma cópia de segurança da versão anterior foi guardada na pasta do sistema.");
    }else{
      if(b) b.innerHTML=`<span>⚠️ ${d.erro||"Falha na atualização."}</span>
        <div class="ub-btns"><button class="ub-later" onclick="fecharBanner()">Fechar</button></div>`;
    }
  }catch(_){
    if(b) b.innerHTML=`<span>⚠️ Erro de conexão ao atualizar.</span>
      <div class="ub-btns"><button class="ub-later" onclick="fecharBanner()">Fechar</button></div>`;
  }
}

async function loadStats(){
  try{
    const s=await (await fetch("/stats")).json();
    const el=document.getElementById("farmStats");
    if(!s.enabled){el.innerHTML="";return;}
    el.innerHTML=
      `<div class="fs"><span class="k">Hoje</span><span class="v">${s.today_jobs??0}</span></div>`
     +`<div class="fs"><span class="k">7 dias</span><span class="v">${s.week_jobs??0}</span></div>`
     +`<div class="fs"><span class="k">Sucesso</span><span class="v">${s.success_rate!=null?s.success_rate+"%":"—"}</span></div>`
     +`<div class="fs"><span class="k">Horas (7d)</span><span class="v">${s.print_hours??0}</span></div>`;
  }catch(_){}
}
loadStats(); setInterval(loadStats,60000);

async function changePassword(){
  const cur=prompt("Senha atual:"); if(cur==null) return;
  const nw=prompt("Nova senha (mínimo 6 caracteres):"); if(nw==null) return;
  try{
    const r=await fetch("/account/password",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({current:cur,new:nw})});
    const d=await r.json().catch(()=>({}));
    alert(d.ok ? "Senha alterada com sucesso." : ("Erro: "+(d.error||"falha")));
  }catch(_){ alert("Erro de conexão."); }
}

/* ── Excluir e reordenar ──────────────────────────────── */
async function removePrinter(name){
  if(!confirm(`Remover a impressora "${name}" do painel?`)) return;
  try{ await fetch("/api/printer/remove",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({name})}); }
  catch(_){ alert("Erro ao remover."); }
}
async function renamePrinter(name){
  const atual=((lastData[name]||{})._meta||{}).apelido||"";
  const novo=prompt(`Nome de exibição para esta impressora:\n(deixe vazio para usar o nome original "${name}")`, atual);
  if(novo===null) return;  // cancelou
  try{
    const r=await fetch("/api/printer/rename",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({name, apelido:novo.trim()})});
    const j=await r.json();
    if(!j.ok){ alert(j.error||"Erro ao renomear."); return; }
    // atualiza na hora
    const disp=novo.trim()||name;
    const r2=cards[name]; if(r2&&r2.pname) r2.pname.textContent=disp;
    if(lastData[name]) lastData[name]._meta=Object.assign(lastData[name]._meta||{},{apelido:novo.trim()});
  }catch(_){ alert("Erro ao renomear."); }
}
function saveOrder(){
  const grid=document.getElementById("grid");
  const order=[...grid.querySelectorAll(".card")].map(c=>c.dataset.name);
  lastOrder=order;
  fetch("/api/printer/reorder",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({order})}).catch(()=>{});
}

/* ── Assistente de adicionar impressora ───────────────── */
let wiz={};
function val(id){ const e=document.getElementById(id); return e?e.value.trim():""; }
function wizErr(msg){ const e=document.getElementById("wizErr"); if(e) e.textContent=msg||""; }
function openAdd(){
  const el=document.getElementById("addOverlay");
  if(!el){ console.error("3DWORK: addOverlay não encontrado — arquivo desatualizado?"); alert("Erro interno: modal não encontrado. O arquivo bambu_dashboard.py pode estar desatualizado. Reinicie o sistema."); return; }
  wiz={step:"brand"}; renderAdd(); el.classList.add("open");
}
function closeAdd(){ document.getElementById("addOverlay").classList.remove("open");
  document.getElementById("addModal").innerHTML=""; }
function wizBrand(b){
  wiz.brand=b;
  if(b==="bambu"){ wiz.step="bambu"; wiz.tab="login"; wiz.needCode=false; renderAdd(); }
  else if(b==="anycubic"){ wiz.step="anycubic"; renderAdd(); }
}
function wizTab(t){ wiz.tab=t; wiz.needCode=false; renderAdd(); }
function wizToggle(i){ if(wiz.sel.has(i)) wiz.sel.delete(i); else wiz.sel.add(i); renderAdd(); }
function wizApelido(i, val){ if(!wiz.apelidos) wiz.apelidos={}; wiz.apelidos[i]=val; }

function renderAdd(){
  const m=document.getElementById("addModal");
  if(wiz.step==="brand"){
    m.innerHTML=`
      <div class="wiz-head"><b>Adicionar impressora</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        <label>Escolha a marca</label>
        <div class="brands">
          <div class="brand-card" onclick="wizBrand('bambu')"><div class="bn">Bambu Lab</div><div class="bs">via nuvem</div></div>
          <div class="brand-card" onclick="wizBrand('anycubic')"><div class="bn">Anycubic / Kobra</div><div class="bs">via rede local</div></div>
        </div>
        <div class="wiz-hint">Bambu: detecta as impressoras da sua conta.<br>Anycubic: busca as impressoras na sua rede local.</div>
      </div>`;
  } else if(wiz.step==="bambu"){
    const tab=wiz.tab||"token";
    m.innerHTML=`
      <div class="wiz-head"><b>Bambu Lab · entrar</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        <div class="auto-detect-box">
          <div class="auto-detect-label">Método rápido — copiar do navegador</div>
          <div class="wiz-hint" style="margin:0">
            <b>1.</b> Abra o <a href="https://makerworld.com" target="_blank" style="color:var(--live)">MakerWorld</a> e faça login.<br>
            <b>2.</b> Aperte <b>F12</b> → aba <b>Network</b> → aperte <b>F5</b>.<br>
            <b>3.</b> Clique na primeira requisição → aba <b>Headers</b> → role até <b>Request Headers</b>.<br>
            <b>4.</b> Copie o valor após <b>token=</b> dentro do campo <b>cookie:</b> (termina antes do próximo <b>;</b>).<br>
            <b>5.</b> Cole abaixo no campo Token.
          </div>
        </div>
        <div class="wiz-divider"><span>ou use e-mail e senha</span></div>
        <div class="wiz-tabs">
          <div class="wiz-tab ${tab==='token'?'on':''}" onclick="wizTab('token')">Token manual</div>
          <div class="wiz-tab ${tab==='login'?'on':''}" onclick="wizTab('login')">E-mail e senha</div>
        </div>
        <label>Região</label>
        <select id="w_region"><option value="us">Global (fora da China)</option><option value="cn">China</option></select>
        ${tab==='token'?`
          <label>Token (cole aqui o valor copiado do Network)</label>
          <input id="w_token" placeholder="AQB...">
          <label>UID (opcional — detectado automaticamente)</label>
          <input id="w_uid" placeholder="u_1234567">
        `:`
          <label>E-mail</label><input id="w_email" value="${wiz.email||''}">
          <label>Senha</label><input id="w_pass" type="password">
          ${wiz.needCode?`<label>Código enviado por e-mail</label><input id="w_code" placeholder="6 dígitos">`:''}
        `}
        <div class="wiz-err" id="wizErr"></div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='brand';renderAdd()">Voltar</button>
        <button class="wiz-btn primary" onclick="detectBambu()">${wiz.needCode?'Confirmar código':'Detectar impressoras'}</button>
      </div>`;
  } else if(wiz.step==="anycubic"){
    m.innerHTML=`
      <div class="wiz-head"><b>Anycubic / Kobra · modo LAN</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        <div class="auto-detect-box">
          <div class="auto-detect-label">Antes de começar</div>
          <div class="wiz-hint" style="margin:0">
            <b>1.</b> Na tela da impressora: <b>Configurações (⚙) → Rede → Modo LAN</b> → ative.<br>
            <b>2.</b> Depois, use a busca automática abaixo — ou digite o IP manualmente
            (ele aparece na mesma tela da impressora).
          </div>
        </div>
        <label>IP da impressora</label>
        <input id="a_ip" placeholder="192.168.1.15">
        <div class="wiz-divider"><span>ou</span></div>
        <button class="auto-detect-btn" onclick="scanAnycubic()" style="width:100%">
          🔍 Buscar impressoras na rede automaticamente
        </button>
        <div class="wiz-hint">Monitoramento local em tempo real. O computador precisa estar na mesma rede da impressora. Enquanto o Modo LAN estiver ativo, o app Anycubic não acompanha essa impressora — o fatiamento pelo Slicer continua funcionando.</div>
        <div class="wiz-err" id="wizErr"></div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='brand';renderAdd()">Voltar</button>
        <button class="wiz-btn primary" onclick="detectAnycubic()">Conectar</button>
      </div>`;
  } else if(wiz.step==="detecting"){
    m.innerHTML=`<div class="wiz-head"><b>Detectando…</b></div><div class="wiz-spin">Consultando sua conta na nuvem…</div>`;
  } else if(wiz.step==="scanning"){
    m.innerHTML=`<div class="wiz-head"><b>Buscando na rede…</b></div>
      <div class="wiz-spin">Procurando impressoras Anycubic na sua rede local.<br>
        <small style="opacity:.7">Isso pode levar até 30 segundos.</small></div>`;
  } else if(wiz.step==="devices"){
    const list=wiz.devices.map((d,i)=>`
      <div class="dev ${wiz.sel.has(i)?'sel':''}" onclick="wizToggle(${i})">
        <div><div class="dn">${d.name}</div><div class="dm">${d.model||'—'} · ${d.serial}</div></div>
        <div class="don ${d.online?'up':'down'}">${d.online?'online':'offline'}</div>
      </div>
      ${wiz.sel.has(i)?`<input class="dev-apelido" placeholder="Apelido (opcional) — ex: Impressora da Sala"
        value="${(wiz.apelidos&&wiz.apelidos[i])||''}" onclick="event.stopPropagation()"
        oninput="wizApelido(${i}, this.value)">`:''}`).join("");
    m.innerHTML=`
      <div class="wiz-head"><b>Impressoras encontradas</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        ${wiz.devices.length
          ?`<label>Selecione as que deseja adicionar</label><div class="dev-list">${list}</div>`
          :`<div class="wiz-hint">Nenhuma impressora encontrada nessa conta.</div>`}
        <div id="devBrowser" style="font-family:'JetBrains Mono',monospace;font-size:.66rem;color:var(--faint);margin-top:.5rem"></div>
        <div class="wiz-err" id="wizErr"></div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='bambu';wiz.needCode=false;renderAdd()">Voltar</button>
        <button class="wiz-btn primary" id="addBtn" onclick="addSelected()" ${wiz.sel.size?'':'disabled'}>Adicionar ${wiz.sel.size||''}</button>
      </div>`;
  }
}

async function autoDetectBrowser(){
  const btn=document.getElementById("autoBtn");
  const errEl=document.getElementById("autoErr");
  if(btn) btn.disabled=true;
  if(btn) btn.textContent="🔍 Detectando…";
  if(errEl) errEl.textContent="";
  try{
    const r=await fetch("/api/bambu/autodetect");
    const d=await r.json();
    if(d.ok){
      wiz.creds={region:d.region||"us", uid:d.uid, token:d.token};
      wiz.devices=d.printers||[]; wiz.sel=new Set(); wiz.step="devices";
      renderAdd();
      // Mostra de qual navegador veio
      const info=document.getElementById("devBrowser");
      if(info) info.textContent=`Token detectado do ${d.browser}.`;
    } else {
      if(errEl) errEl.textContent=d.error||"Não foi possível detectar. Use o login manual abaixo.";
      if(btn){ btn.disabled=false; btn.textContent="🔍 Detectar login do MakerWorld no navegador"; }
    }
  }catch(_){
    if(errEl) errEl.textContent="Erro de conexão com o servidor.";
    if(btn){ btn.disabled=false; btn.textContent="🔍 Detectar login do MakerWorld no navegador"; }
  }
}

async function detectBambu(){
  const region=val("w_region")||"us";
  const body={brand:"bambu",region};
  if(wiz.tab==="token"){ body.uid=val("w_uid"); body.token=val("w_token"); }
  else { body.email=val("w_email"); wiz.email=body.email; body.password=val("w_pass");
    if(wiz.needCode) body.code=val("w_code"); }
  wiz.step="detecting"; renderAdd();
  try{
    const r=await fetch("/api/detect",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(d.need_code){ wiz.step="bambu"; wiz.needCode=true; renderAdd();
      wizErr("Enviamos um código para o seu e-mail. Digite-o acima e confirme."); return; }
    if(!d.ok){ wiz.step="bambu"; renderAdd(); wizErr(d.error||"Falha na detecção."); return; }
    wiz.creds={region:d.region,uid:d.uid,token:d.token};
    wiz.devices=d.printers||[]; wiz.sel=new Set(); wiz.step="devices"; renderAdd();
  }catch(_){ wiz.step="bambu"; renderAdd(); wizErr("Erro de conexão."); }
}

async function detectAnycubic(){
  const ip=val("a_ip");
  if(!ip){ wizErr("Digite o IP da impressora primeiro."); return; }
  wiz.step="detecting"; renderAdd();
  try{
    const r=await fetch("/api/detect",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({brand:"anycubic",ip})});
    const d=await r.json();
    if(!d.ok){ wiz.step="anycubic"; renderAdd(); wizErr(d.error||"Falha na detecção."); return; }
    wiz.brand="anycubic";
    wiz.creds={ip};
    wiz.devices=d.printers||[]; wiz.sel=new Set(); wiz.step="devices"; renderAdd();
  }catch(_){ wiz.step="anycubic"; renderAdd(); wizErr("Erro de conexão."); }
}

async function scanAnycubic(){
  wiz.step="scanning"; renderAdd();
  try{
    const r=await fetch("/api/detect",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({brand:"anycubic", scan:true})});
    const d=await r.json();
    if(!d.ok){ wiz.step="anycubic"; renderAdd(); wizErr(d.error||"Nenhuma impressora encontrada."); return; }
    wiz.brand="anycubic";
    wiz.creds={};
    wiz.devices=d.printers||[]; wiz.sel=new Set(); wiz.step="devices"; renderAdd();
  }catch(_){ wiz.step="anycubic"; renderAdd(); wizErr("Erro ao buscar na rede."); }
}

async function addSelected(){
  const btn=document.getElementById("addBtn"); if(btn) btn.disabled=true;
  const errs=[];
  for(const i of wiz.sel){
    const d=wiz.devices[i];
    const apelido=((wiz.apelidos&&wiz.apelidos[i])||"").trim();
    let cfg;
    if(wiz.brand==="anycubic"){
      cfg={brand:"anycubic",mode:"lan",name:d.name,serial:d.ip||d.serial,
        ip:d.ip||d.serial,printer_id:d.printer_id||d.serial,model:d.model,apelido};
    } else {
      cfg={brand:"bambu",mode:"cloud",name:d.name,serial:d.serial,model:d.model,
        region:wiz.creds.region,uid:wiz.creds.uid,token:wiz.creds.token,apelido};
    }
    try{
      const r=await fetch("/api/printer/add",{method:"POST",
        headers:{"Content-Type":"application/json"},body:JSON.stringify(cfg)});
      const j=await r.json(); if(!j.ok) errs.push(`${d.name}: ${j.error}`);
    }catch(_){ errs.push(`${d.name}: erro de conexão`); }
  }
  if(errs.length){ wizErr(errs.join(" · ")); if(btn) btn.disabled=false; }
  else closeAdd();
}
</script>
</body>
</html>"""


if __name__ == "__main__":
    import sys as _sys

    # Modo --init-only: usado pelo instalador para gerar auth.json e salvar a senha
    # num arquivo temporário sem subir o servidor.
    if "--init-only" in _sys.argv:
        cred_file = Path(__file__).with_name("first_run_password.txt")
        # Se o auth.json foi criado agora (primeira execução), a senha já está
        # em AUTH['_plain'] que colocamos abaixo. Caso contrário, já existia.
        plain = AUTH.get("_plain")
        if plain:
            cred_file.write_text(plain)
            # Remove a senha em texto claro da memória
            AUTH.pop("_plain", None)
            print(f"[init-only] Credenciais geradas. Senha salva em {cred_file}")
        else:
            print("[init-only] auth.json já existia — mantendo credenciais.")
        _sys.exit(0)
    PRINTERS_CFG[:] = load_printers()
    _sync_order()
    print(f"[init] {len(PRINTERS_CFG)} impressora(s) configurada(s).")
    # Verifica a licença
    lic = refresh_license()
    if lic.get("ok"):
        print(f"[licenca] ativa — cliente: {lic.get('cliente','')}")
    else:
        print(f"[licenca] SEM LICENÇA VÁLIDA ({lic.get('reason')})")
        print(f"[licenca] Código da máquina: {lic.get('fingerprint')}")
        print("[licenca] Acesse http://localhost:8000 para ativar.")
    for cfg in list(PRINTERS_CFG):
        start_printer(cfg)
    print("[init] Dashboard em http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
