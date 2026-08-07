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
import platform
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
APP_VERSION = "5.7"

# Mural de atualizações — o que cada versão trouxe, em linguagem para o
# usuário final. A cada versão nova, adicione um bloco no TOPO desta lista.
# "data" é opcional (só para exibição). "novidades" é uma lista de frases.
CHANGELOG = [
    {"versao": "5.7", "data": "2026-08",
     "titulo": "Flashforge: busca na rede",
     "novidades": [
         "Agora da para buscar as Flashforge na rede automaticamente (acha varias de uma vez).",
         "Ou adicionar pelo IP, uma a uma.",
     ]},
    {"versao": "5.6", "data": "2026-08",
     "titulo": "Suporte a impressoras Flashforge (AD5X)",
     "novidades": [
         "Agora da para adicionar impressoras Flashforge AD5X (via rede local).",
         "Mostra status, temperatura, progresso e as cores dos 4 filamentos.",
         "Controle de pausar, retomar e cancelar.",
         "Obs: a AD5X nao tem camera, entao o video nao fica disponivel.",
     ]},
    {"versao": "5.5", "data": "2026-08",
     "titulo": "Novo logo FarmSync",
     "novidades": [
         "Novo logo do FarmSync na tela de entrada e no painel.",
     ]},
    {"versao": "5.4", "data": "2026-07",
     "titulo": "Novo nome: FarmSync",
     "novidades": [
         "O sistema agora se chama FarmSync.",
     ]},
    {"versao": "5.3", "data": "2026-07",
     "titulo": "Ignorar erro fantasma da Bambu",
     "novidades": [
         "O erro 0x500C011 (fantasma, nao documentado pela Bambu) nao aparece mais.",
         "Os demais erros continuam sendo mostrados normalmente.",
     ]},
    {"versao": "5.2", "data": "2026-07",
     "titulo": "Controle de estoque de filamento",
     "novidades": [
         "Nova pagina Estoque: cadastre filamentos por marca, tipo, cor e quantidade.",
         "Ao registrar o custo de uma impressao, escolha o filamento e desconte do estoque.",
         "Alertas automaticos quando o filamento chega a 10kg, 5kg e 1kg.",
         "Relatorios de consumo com filtro por dia/semana/mes/ano, filamento e impressoras.",
     ]},
    {"versao": "5.1", "data": "2026-07",
     "titulo": "Preparo para cor de filamento na Anycubic",
     "novidades": [
         "Ferramenta interna para investigar os dados da Kobra (cor do filamento).",
     ]},
    {"versao": "5.0", "data": "2026-07",
     "titulo": "Camera sem a janela preta do CMD",
     "novidades": [
         "Corrigido: nao abre mais a janela preta (ffmpeg) ao ver a camera.",
     ]},
    {"versao": "4.9", "data": "2026-07",
     "titulo": "Cor do filamento nas Bambu sem AMS",
     "novidades": [
         "Agora mostra a cor do filamento tambem nas Bambu sem AMS (bobina externa).",
         "Aparece a cor mesmo que o tipo nao esteja definido na impressora.",
     ]},
    {"versao": "4.8", "data": "2026-07",
     "titulo": "Cards das impressoras mais compactos",
     "novidades": [
         "Reduzido o espaco em branco dos cards, sem diminuir letras nem informacoes.",
         "Cards ocupam menos altura, cabe mais na tela.",
     ]},
    {"versao": "4.7", "data": "2026-07",
     "titulo": "Orcamentos repaginados",
     "novidades": [
         "Tela de orcamentos com letras maiores e mais faceis de ler.",
         "Campos mais confortaveis, com mais espaco e melhor visual.",
         "Cards de resumo e total maiores e mais destacados.",
     ]},
    {"versao": "4.6", "data": "2026-07",
     "titulo": "Avisos quando a impressao termina ou falha",
     "novidades": [
         "Novo: o sistema avisa quando uma impressao termina ou falha.",
         "Aviso por som e por notificacao na tela (Windows).",
         "Voce escolhe o que quer receber em Configuracoes > Avisos de impressao.",
     ]},
    {"versao": "4.5", "data": "2026-07",
     "titulo": "Contador Imprimindo agora atualiza ao vivo",
     "novidades": [
         "O card 'Imprimindo agora' agora atualiza sozinho quando uma impressao comeca.",
         "Antes o numero ficava congelado se voce ja estivesse na tela.",
     ]},
    {"versao": "4.4", "data": "2026-07",
     "titulo": "Correcao no contador de impressoras",
     "novidades": [
         "O card 'Imprimindo agora' agora conta certo, igual aos cards das impressoras.",
     ]},
    {"versao": "4.3", "data": "2026-07",
     "titulo": "Mensagem de atualizacao mais amigavel",
     "novidades": [
         "Nova mensagem de parabens ao concluir a atualizacao.",
     ]},
    {"versao": "4.2", "data": "2026-07",
     "titulo": "Orcamento: filamento por preco do rolo",
     "novidades": [
         "Agora voce informa o preco do rolo (R$/kg) e o sistema calcula o custo pelo peso.",
         "Corrigido o calculo do custo do filamento no orcamento.",
         "Removidas as setinhas dos campos de numero, que atrapalhavam ao digitar.",
     ]},
    {"versao": "4.1", "data": "2026-07",
     "titulo": "Atualizacao: so reiniciar o computador",
     "novidades": [
         "Ao atualizar, basta reiniciar o computador para usar a versao nova.",
         "Ao ligar de novo, o sistema abre sozinho ja atualizado.",
     ]},
    {"versao": "4.0", "data": "2026-07",
     "titulo": "Atualizacao mais simples e segura",
     "novidades": [
         "Ao atualizar, o sistema avisa para fechar e abrir - simples e sem travar.",
         "Removido o reinicio automatico, que as vezes prendia a tela.",
     ]},
    {"versao": "3.9", "data": "2026-07",
     "titulo": "Reinicio automatico mais confiavel",
     "novidades": [
         "Corrigido o reinicio apos atualizar quando o sistema roda escondido.",
         "A tela de atualizacao nao fica mais presa.",
     ]},
    {"versao": "3.8", "data": "2026-07",
     "titulo": "Botoes de controle mais discretos",
     "novidades": [
         "Os botoes de pausar e cancelar ficaram menores e menos chamativos.",
     ]},
    {"versao": "3.7", "data": "2026-07",
     "titulo": "Controle das impressoras: pausar e cancelar",
     "novidades": [
         "Agora da para pausar, retomar e cancelar a impressao pelo painel.",
         "O cancelamento pede confirmacao antes, para evitar engano.",
         "Nas Bambu funciona direto; nas Anycubic LAN, depende do modelo.",
     ]},
    {"versao": "3.6", "data": "2026-07",
     "titulo": "Ajuste nos campos do orcamento",
     "novidades": [
         "Campos do orcamento com tamanhos melhores: tempo agora da para ler bem.",
         "Peso, filamento, quantidade e valor de venda mais compactos.",
     ]},
    {"versao": "3.5", "data": "2026-07",
     "titulo": "Correcao na atualizacao automatica",
     "novidades": [
         "Corrigido o travamento na tela 'Aplicando a nova versao'.",
         "A atualizacao agora espera a hora certa para reiniciar, sem conflito.",
         "Botao 'Recarregar agora' sempre disponivel durante a atualizacao.",
     ]},
    {"versao": "3.4", "data": "2026-07",
     "titulo": "Calculadora mais simples",
     "novidades": [
         "Removido o botão de puxar tempo da impressão na calculadora.",
     ]},
    {"versao": "3.3", "data": "2026-07",
     "titulo": "Seu logo no PDF do orçamento",
     "novidades": [
         "Se você usa seu próprio logo, ele aparece no PDF do orçamento.",
         "Se ainda usa o logo padrão, o orçamento sai sem logo (mais neutro para o seu cliente).",
     ]},
    {"versao": "3.2", "data": "2026-07",
     "titulo": "Orçamentos: valor de venda e margem separada",
     "novidades": [
         "Agora você digita o valor de venda de cada peça (o preço do cliente).",
         "O sistema mostra só para você o seu custo real e o lucro/margem.",
         "Novas colunas: peso em g ou kg, e o valor do filamento por peça.",
         "O PDF do cliente sai limpo, só com descrição, quantidade e valores.",
     ]},
    {"versao": "3.1", "data": "2026-07",
     "titulo": "Orçamentos: data e tempo em min ou horas",
     "novidades": [
         "Novo campo de data no topo do orçamento (você escolhe a data).",
         "No tempo de cada peça, dá para usar minutos OU horas.",
         "A coluna 'Acab.' agora aparece como 'Acabamento (R$)', mais clara.",
     ]},
    {"versao": "3.0", "data": "2026-07",
     "titulo": "Atualização automática, sem reiniciar na mão",
     "novidades": [
         "Ao atualizar, o sistema agora reinicia sozinho e já abre na versão nova.",
         "Não precisa mais fechar e abrir o programa manualmente.",
     ]},
    {"versao": "2.9", "data": "2026-07",
     "titulo": "Acesso remoto com endereço fixo",
     "novidades": [
         "Agora o acesso remoto pode ter um endereço fixo, que nunca muda.",
         "Opção de ligar o acesso remoto automaticamente ao abrir o sistema.",
     ]},
    {"versao": "2.8", "data": "2026-07",
     "titulo": "Acesso remoto — veja de qualquer lugar",
     "novidades": [
         "Novo: acesse suas impressoras de fora da sua rede, no celular ou em outro PC.",
         "Métricas ao vivo e câmera funcionam remotamente, com login e endereço seguro.",
         "Não precisa mexer no roteador nem liberar portas — é só ativar em Configurações.",
     ]},
    {"versao": "2.7", "data": "2026-07",
     "titulo": "Logo nos relatórios e identificação no topo",
     "novidades": [
         "Seu logo agora aparece também nos relatórios em PDF (além da tela de entrada e do menu).",
         "O nome do sistema aparece no topo do painel.",
     ]},
    {"versao": "2.6", "data": "2026-07",
     "titulo": "Painel de parede completo",
     "novidades": [
         "O modo painel de parede agora mostra todas as informações das impressoras: progresso, temperaturas, filamentos e todos os tempos.",
     ]},
    {"versao": "2.5", "data": "2026-07",
     "titulo": "Mural de atualizações",
     "novidades": [
         "Nova página no menu que mostra tudo o que cada atualização trouxe.",
     ]},
    {"versao": "2.4", "data": "2026-07",
     "titulo": "Dashboard mais tranquila",
     "novidades": [
         "As animações da tela inicial agora acontecem uma vez ao abrir, sem ficar repetindo.",
         "Os dados das impressoras continuam atualizando ao vivo, mas sem piscar na tela.",
     ]},
    {"versao": "2.3", "data": "2026-07",
     "titulo": "Licenças mais seguras e por período",
     "novidades": [
         "As licenças agora podem ter validade em meses (ex.: 1 ano).",
         "O sistema avisa quando a licença está perto de vencer.",
         "Corrigido um problema em que a licença podia pedir reativação sem motivo.",
     ]},
    {"versao": "2.2", "data": "2026-07",
     "titulo": "Login Bambu para contas Google",
     "novidades": [
         "Quem criou a conta Bambu com o Google (ou Apple/Facebook) agora entra pelo código enviado ao e-mail.",
     ]},
    {"versao": "2.0", "data": "2026-07",
     "titulo": "Conexão com a Bambu pela conta, mais simples",
     "novidades": [
         "Entrar na conta Bambu agora é só com e-mail e senha — sem passos técnicos.",
         "Se a Bambu pedir verificação, o código chega no seu e-mail.",
         "Suporte a contas com verificação em duas etapas.",
     ]},
    {"versao": "1.9", "data": "2026-07",
     "titulo": "Adicionar Bambu pelo IP ficou mais fácil",
     "novidades": [
         "O número de série agora é opcional — o sistema descobre sozinho pelo IP.",
         "Botão para buscar as impressoras Bambu automaticamente na rede.",
     ]},
    {"versao": "1.6", "data": "2026-07",
     "titulo": "Impressoras Bambu pela rede local",
     "novidades": [
         "Agora dá para adicionar uma Bambu direto pelo IP, com teste de conexão.",
     ]},
    {"versao": "1.5", "data": "2026-07",
     "titulo": "Nova identidade visual",
     "novidades": [
         "Novo logo 3DWORK na tela de entrada e no painel.",
     ]},
    {"versao": "1.4", "data": "2026-07",
     "titulo": "Acesso mais simples",
     "novidades": [
         "Primeiro acesso com usuário e senha 'admin', com aviso para trocar depois.",
     ]},
    {"versao": "1.3", "data": "2026-07",
     "titulo": "Orçamentos",
     "novidades": [
         "Nova página para criar orçamentos de impressão e gerar PDF para o cliente.",
     ]},
    {"versao": "1.1", "data": "2026-07",
     "titulo": "Gerenciador de projetos e atualizações",
     "novidades": [
         "Nova página de Projetos para organizar seus arquivos STL.",
         "O sistema passa a avisar quando há uma versão nova disponível.",
     ]},
]

# URL do arquivo que informa a versão mais recente publicada (GitHub raw).
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


def controlar_impressao(name, acao):
    """Envia um comando de controle para a impressora: 'pause', 'resume' ou
    'stop' (cancelar). Retorna (ok, mensagem)."""
    handle = PRINTERS.get(name)
    if not handle:
        return False, "Impressora não está conectada."
    brand = handle.get("brand")
    if acao not in ("pause", "resume", "stop"):
        return False, "Ação inválida."

    try:
        if brand == "bambu":
            client = handle.get("client")
            topic = handle.get("request_topic")
            if not client or not topic:
                return False, "Impressora Bambu não está pronta."
            # comandos do protocolo Bambu (print.command)
            cmd = {"pause": "pause", "resume": "resume", "stop": "stop"}[acao]
            payload = {"print": {"sequence_id": "0", "command": cmd}}
            client.publish(topic, json.dumps(payload))
            return True, None

        elif brand == "anycubic":
            holder = handle.get("client")
            if not isinstance(holder, dict) or not holder.get("client"):
                return False, "Impressora Anycubic não está pronta."
            base = holder.get("base")
            mqtt_client = holder.get("client")
            if not base:
                return False, "Impressora Anycubic não está pronta."
            # protocolo Anycubic LAN: comando de print em {base}/print
            # (o campo pode variar por modelo/firmware — testar na impressora)
            codigo = {"pause": 1, "resume": 2, "stop": 3}[acao]
            req = {"type": "print", "action": codigo}
            mqtt_client.publish(f"{base}/print", json.dumps(req))
            return True, ("Comando enviado. Se a impressora não responder, "
                          "pode ser que o modelo dela não aceite controle "
                          "remoto em modo LAN.")

        elif brand == "flashforge":
            ip = handle.get("ip")
            serial = handle.get("serial", "")
            check_code = handle.get("check_code", "")
            if not ip:
                return False, "Impressora Flashforge não está pronta."
            # A Flashforge usa /control com comandos de estado.
            import requests
            cmd_map = {
                "pause": {"cmd": "printerCtl_cmd", "args": {"action": "pause"}},
                "resume": {"cmd": "printerCtl_cmd", "args": {"action": "continue"}},
                "stop": {"cmd": "printerCtl_cmd", "args": {"action": "cancel"}},
            }
            body = {"serialNumber": serial, "checkCode": check_code,
                    "payload": cmd_map[acao]}
            try:
                r = requests.post(f"http://{ip}:8898/control", json=body, timeout=6)
                if r.status_code == 200:
                    return True, None
                return False, f"A impressora respondeu com erro ({r.status_code})."
            except Exception as exc:
                return False, f"Não consegui falar com a impressora: {exc}"
        else:
            return False, "Marca de impressora desconhecida."
    except Exception as exc:
        return False, f"Falha ao enviar o comando: {exc}"


# ---------------------------------------------------------------------------
# Estado compartilhado entre as threads MQTT e o servidor web
# ---------------------------------------------------------------------------
STATE = {}          # nome_da_impressora -> dict de estado mesclado
STATE_LOCK = threading.Lock()

# Diagnóstico dos dados brutos da Anycubic (para investigar campos como a cor).
_ANYCUBIC_DEBUG = {"on": False, "arquivo": "anycubic_debug.txt"}


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
            # Estoque de filamento: cada item é uma combinação marca+tipo+cor.
            con.execute("""
                CREATE TABLE IF NOT EXISTS estoque (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    marca TEXT NOT NULL,
                    tipo TEXT NOT NULL,
                    cor TEXT NOT NULL,
                    cor_hex TEXT,
                    saldo_g REAL NOT NULL DEFAULT 0,
                    preco_kg REAL NOT NULL DEFAULT 0,
                    criado_em TEXT,
                    UNIQUE(marca, tipo, cor)
                )
            """)
            # Movimentos de estoque: cada entrada (compra) ou saída (impressão).
            con.execute("""
                CREATE TABLE IF NOT EXISTS estoque_mov (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    tipo_mov TEXT NOT NULL,          -- 'entrada' ou 'saida'
                    gramas REAL NOT NULL,
                    custo REAL,                       -- custo desta saída (R$)
                    printer TEXT,                     -- impressora (nas saídas)
                    obs TEXT,
                    data TEXT NOT NULL
                )
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_mov_data ON estoque_mov(data)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_mov_item ON estoque_mov(item_id)")
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


# ══════════════════════════════════════════════════════════════════
#  ESTOQUE DE FILAMENTO
# ══════════════════════════════════════════════════════════════════
_ESTOQUE_LOCK = threading.Lock()
# Níveis de alerta (em gramas): avisa quando o saldo cruza para baixo destes.
_ALERTA_NIVEIS_G = [10000, 5000, 1000]   # 10kg, 5kg, 1kg


def _estoque_con():
    con = sqlite3.connect(str(Path(__file__).with_name("historico.db")))
    con.row_factory = sqlite3.Row
    return con


def estoque_listar():
    """Lista todos os itens de estoque com saldo e situação de alerta."""
    try:
        with _ESTOQUE_LOCK:
            con = _estoque_con()
            rows = con.execute("""
                SELECT * FROM estoque ORDER BY marca, tipo, cor
            """).fetchall()
            con.close()
        itens = []
        for r in rows:
            saldo = float(r["saldo_g"] or 0)
            itens.append({
                "id": r["id"], "marca": r["marca"], "tipo": r["tipo"],
                "cor": r["cor"], "cor_hex": r["cor_hex"] or "",
                "saldo_g": round(saldo, 1),
                "saldo_kg": round(saldo / 1000.0, 3),
                "preco_kg": float(r["preco_kg"] or 0),
                "alerta": _nivel_alerta(saldo),
            })
        return itens
    except Exception as exc:
        print("[estoque] erro ao listar:", exc)
        return []


def _nivel_alerta(saldo_g):
    """Retorna o nível de alerta do saldo: 'critico' (<1kg), 'baixo' (<5kg),
    'atencao' (<10kg), 'negativo', ou None."""
    if saldo_g < 0:
        return "negativo"
    if saldo_g <= 1000:
        return "critico"
    if saldo_g <= 5000:
        return "baixo"
    if saldo_g <= 10000:
        return "atencao"
    return None


def estoque_add_item(marca, tipo, cor, cor_hex, kg, preco_kg):
    """Cadastra um novo filamento OU adiciona quantidade a um já existente."""
    marca = (marca or "").strip()
    tipo = (tipo or "").strip()
    cor = (cor or "").strip()
    if not (marca and tipo and cor):
        return False, "Preencha marca, tipo e cor."
    gramas = _f(kg) * 1000.0
    preco = _f(preco_kg)
    try:
        with _ESTOQUE_LOCK:
            con = _estoque_con()
            ex = con.execute("SELECT id, saldo_g FROM estoque WHERE marca=? AND tipo=? AND cor=?",
                             (marca, tipo, cor)).fetchone()
            agora = datetime.now().isoformat()
            if ex:
                # já existe: soma a quantidade e atualiza o preço
                novo = float(ex["saldo_g"] or 0) + gramas
                con.execute("UPDATE estoque SET saldo_g=?, preco_kg=?, cor_hex=? WHERE id=?",
                            (novo, preco, cor_hex or "", ex["id"]))
                item_id = ex["id"]
            else:
                cur = con.execute("""INSERT INTO estoque(marca,tipo,cor,cor_hex,saldo_g,preco_kg,criado_em)
                                     VALUES(?,?,?,?,?,?,?)""",
                                  (marca, tipo, cor, cor_hex or "", gramas, preco, agora))
                item_id = cur.lastrowid
            if gramas != 0:
                con.execute("""INSERT INTO estoque_mov(item_id,tipo_mov,gramas,data,obs)
                               VALUES(?,?,?,?,?)""",
                            (item_id, "entrada", gramas, agora, "Cadastro/reposição"))
            con.commit()
            con.close()
        return True, None
    except Exception as exc:
        return False, f"Erro ao salvar: {exc}"


def estoque_editar(item_id, marca, tipo, cor, cor_hex, saldo_kg, preco_kg):
    """Edita os dados de um item (inclusive ajuste manual de saldo)."""
    try:
        with _ESTOQUE_LOCK:
            con = _estoque_con()
            con.execute("""UPDATE estoque SET marca=?,tipo=?,cor=?,cor_hex=?,saldo_g=?,preco_kg=?
                           WHERE id=?""",
                        ((marca or "").strip(), (tipo or "").strip(), (cor or "").strip(),
                         cor_hex or "", _f(saldo_kg) * 1000.0, _f(preco_kg), item_id))
            con.commit(); con.close()
        return True, None
    except Exception as exc:
        return False, f"Erro ao editar: {exc}"


def estoque_remover(item_id):
    try:
        with _ESTOQUE_LOCK:
            con = _estoque_con()
            con.execute("DELETE FROM estoque WHERE id=?", (item_id,))
            con.execute("DELETE FROM estoque_mov WHERE item_id=?", (item_id,))
            con.commit(); con.close()
        return True, None
    except Exception as exc:
        return False, f"Erro ao remover: {exc}"


def estoque_descontar(item_id, gramas, printer=None, obs=None):
    """Desconta gramas de um item (saída por impressão). Calcula o custo pelo
    preço do rolo. Deixa o saldo ficar negativo (destacado em vermelho)."""
    gramas = _f(gramas)
    if gramas <= 0:
        return False, "Informe as gramas usadas.", None
    try:
        with _ESTOQUE_LOCK:
            con = _estoque_con()
            it = con.execute("SELECT * FROM estoque WHERE id=?", (item_id,)).fetchone()
            if not it:
                con.close()
                return False, "Filamento não encontrado no estoque.", None
            preco_kg = float(it["preco_kg"] or 0)
            custo = (gramas / 1000.0) * preco_kg
            novo = float(it["saldo_g"] or 0) - gramas
            agora = datetime.now().isoformat()
            con.execute("UPDATE estoque SET saldo_g=? WHERE id=?", (novo, item_id))
            con.execute("""INSERT INTO estoque_mov(item_id,tipo_mov,gramas,custo,printer,obs,data)
                           VALUES(?,?,?,?,?,?,?)""",
                        (item_id, "saida", gramas, custo, printer, obs, agora))
            con.commit()
            info = {"custo": round(custo, 2), "saldo_g": round(novo, 1),
                    "marca": it["marca"], "tipo": it["tipo"], "cor": it["cor"],
                    "alerta": _nivel_alerta(novo)}
            con.close()
        return True, None, info
    except Exception as exc:
        return False, f"Erro ao descontar: {exc}", None


def estoque_relatorio(inicio=None, fim=None, printers=None, item_id=None):
    """Relatório de consumo (saídas) por período, impressoras e/ou item.
    Retorna totais por filamento e o total geral (gramas e custo)."""
    try:
        con = _estoque_con()
        q = """SELECT m.*, e.marca, e.tipo, e.cor, e.cor_hex
               FROM estoque_mov m JOIN estoque e ON e.id=m.item_id
               WHERE m.tipo_mov='saida'"""
        params = []
        if inicio:
            q += " AND m.data >= ?"; params.append(inicio)
        if fim:
            q += " AND m.data <= ?"; params.append(fim)
        if item_id:
            q += " AND m.item_id = ?"; params.append(item_id)
        if printers:
            marks = ",".join("?" * len(printers))
            q += f" AND m.printer IN ({marks})"; params.extend(printers)
        rows = con.execute(q, params).fetchall()
        con.close()
        # agrupa por filamento
        grupos = {}
        tot_g = 0.0; tot_c = 0.0
        for r in rows:
            chave = (r["marca"], r["tipo"], r["cor"])
            g = grupos.setdefault(chave, {
                "marca": r["marca"], "tipo": r["tipo"], "cor": r["cor"],
                "cor_hex": r["cor_hex"] or "", "gramas": 0.0, "custo": 0.0, "impressoes": 0})
            g["gramas"] += float(r["gramas"] or 0)
            g["custo"] += float(r["custo"] or 0)
            g["impressoes"] += 1
            tot_g += float(r["gramas"] or 0)
            tot_c += float(r["custo"] or 0)
        itens = sorted(grupos.values(), key=lambda x: -x["gramas"])
        for g in itens:
            g["gramas"] = round(g["gramas"], 1)
            g["kg"] = round(g["gramas"] / 1000.0, 3)
            g["custo"] = round(g["custo"], 2)
        return {"itens": itens, "total_gramas": round(tot_g, 1),
                "total_kg": round(tot_g / 1000.0, 3), "total_custo": round(tot_c, 2),
                "total_impressoes": len(rows)}
    except Exception as exc:
        print("[estoque] erro no relatório:", exc)
        return {"itens": [], "total_gramas": 0, "total_kg": 0, "total_custo": 0, "total_impressoes": 0}


# ---------------------------------------------------------------------------
# Autenticação (login por usuário e senha). Guarda credenciais em auth.json.
# Na primeira execução cria o usuário "admin" com a senha padrão "admin".
# Enquanto a senha padrão estiver em uso, o painel mostra um aviso pedindo
# para trocá-la (o sistema fica acessível a toda a rede local).
# ---------------------------------------------------------------------------
AUTH_PATH = Path(__file__).with_name("auth.json")
SESSION_COOKIE = "farm_session"
SESSION_TTL = 7 * 24 * 3600          # 7 dias
_SESSIONS = {}                        # sid -> expiração (epoch)
SENHA_PADRAO = "admin"


def _hash_pw(password, salt):
    return hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 200_000).hex()


def load_or_create_auth():
    if AUTH_PATH.exists():
        try:
            return json.loads(AUTH_PATH.read_text())
        except Exception as exc:
            print("[auth] auth.json inválido:", exc)
    # primeira execução: cria admin com a senha padrão
    salt = secrets.token_hex(16)
    data = {"username": "admin", "salt": salt,
            "pw_hash": _hash_pw(SENHA_PADRAO, salt),
            "senha_padrao": True}
    AUTH_PATH.write_text(json.dumps(data, indent=2))
    print("\n" + "=" * 56)
    print("  ACESSO AO PAINEL")
    print("  usuário: admin")
    print("  senha:   admin")
    print("  (troque a senha dentro do painel, em Configurações)")
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


def _flashforge_detail(ip, serial, check_code, timeout=6):
    """Consulta o endpoint /detail da Flashforge (porta 8898) e retorna o JSON.
    A AD5X/5M usam HTTP REST com autenticação por CheckCode."""
    import requests
    url = f"http://{ip}:8898/detail"
    payload = {"serialNumber": serial or "", "checkCode": check_code or ""}
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def flashforge_translate(detail):
    """Converte a resposta /detail da Flashforge para o formato interno
    (mesmos campos que o resto do sistema usa: gcode_state, mc_percent, etc.)."""
    if not isinstance(detail, dict):
        return None
    d = detail.get("detail", detail)  # o payload real vem em "detail"

    # Estado da impressora → mapeia para os estados internos
    status = str(d.get("status", "") or d.get("machineStatus", "")).lower()
    estado_map = {
        "ready": "IDLE", "idle": "IDLE",
        "printing": "RUNNING", "busy": "RUNNING",
        "pause": "PAUSE", "paused": "PAUSE",
        "completed": "FINISH", "finish": "FINISH", "finished": "FINISH",
        "cancel": "FAILED", "error": "FAILED", "failed": "FAILED",
        "heating": "PREPARE", "prepare": "PREPARE",
    }
    gcode_state = estado_map.get(status, "IDLE")

    # Progresso: a Flashforge dá 0..1 (fração) ou 0..100 conforme firmware
    prog = d.get("printProgress")
    if prog is None:
        prog = d.get("progress", 0)
    try:
        prog = float(prog)
        mc_percent = round(prog * 100) if prog <= 1.0 else round(prog)
    except Exception:
        mc_percent = 0

    # Camadas
    layer = d.get("currentLayer") or d.get("printLayer") or 0
    total_layer = d.get("targetLayer") or d.get("totalLayer") or 0

    # Temperaturas
    def _temp(v):
        try: return float(v)
        except Exception: return 0.0
    nozzle = _temp(d.get("rightTemp") or d.get("nozzleTemp") or d.get("extruderTemp") or 0)
    nozzle_t = _temp(d.get("rightTargetTemp") or d.get("nozzleTargetTemp") or 0)
    bed = _temp(d.get("platTemp") or d.get("bedTemp") or 0)
    bed_t = _temp(d.get("platTargetTemp") or d.get("bedTargetTemp") or 0)

    # Tempo restante (segundos → minutos)
    rem = d.get("remainingTime") or d.get("printRemainingTime") or 0
    try:
        rem_min = round(float(rem) / 60) if float(rem) > 300 else round(float(rem))
    except Exception:
        rem_min = 0

    # Nome do trabalho
    job = d.get("printFileName") or d.get("fileName") or d.get("jobName") or ""

    out = {
        "gcode_state": gcode_state,
        "mc_percent": mc_percent,
        "layer_num": int(layer or 0),
        "total_layer_num": int(total_layer or 0),
        "nozzle_temper": nozzle, "nozzle_target_temper": nozzle_t,
        "bed_temper": bed, "bed_target_temper": bed_t,
        "mc_remaining_time": rem_min,
        "subtask_name": job,
    }

    # Estação de filamento (IFS) → vira "ams" no formato interno, com as cores
    ms = d.get("matlStationInfo")
    if isinstance(ms, dict) and d.get("hasMatlStation"):
        slots = ms.get("slotInfos", []) or []
        trays = []
        for s in slots:
            cor = s.get("materialColor", "") or ""
            hexcol = cor.lstrip("#").upper() + "FF" if cor else "00000000"
            trays.append({
                "id": str(s.get("slotId", 0)),
                "tray_color": hexcol,
                "tray_type": s.get("materialName", "") if s.get("hasFilament") else "",
            })
        cur_slot = ms.get("currentSlot", 0)
        out_ams = {"ams": [{"id": "0", "tray": trays}], "tray_now": str(cur_slot)}
        out["_ams"] = out_ams

    # Código de erro (se houver)
    err = d.get("errorCode")
    if err and str(err) not in ("", "0", "E0000"):
        out["print_error_str"] = str(err)

    return out


def start_flashforge_printer(cfg: dict):
    """Monitora uma Flashforge (AD5X/5M) via HTTP REST (porta 8898)."""
    name = cfg["name"]
    ip = cfg.get("ip") or ""
    serial = cfg.get("serial", "")
    check_code = cfg.get("check_code") or cfg.get("access_code") or ""

    with STATE_LOCK:
        STATE[name] = {"_meta": {"name": name, "online": False,
                                 "apelido": cfg.get("apelido", ""),
                                 "brand": "flashforge",
                                 "mode": "lan",
                                 "ip": ip,
                                 "model": cfg.get("model", "AD5X"),
                                 "has_camera": False}}  # AD5X não tem câmera

    stop_flag = threading.Event()

    def run():
        while not stop_flag.is_set():
            detail = _flashforge_detail(ip, serial, check_code)
            if detail is None:
                with STATE_LOCK:
                    STATE.setdefault(name, {"_meta": {}})["_meta"]["online"] = False
                broadcaster.notify_from_thread()
                stop_flag.wait(10)
                continue

            translated = flashforge_translate(detail)
            if translated:
                ams = translated.pop("_ams", None)
                with STATE_LOCK:
                    st = STATE.setdefault(name, {"_meta": {}})
                    st["_meta"]["online"] = True
                    st["_meta"]["_raw"] = detail
                    st.setdefault("print", {}).update(translated)
                    if ams:
                        st["ams"] = ams
                    cur_print = dict(st.get("print", {}))
                history_db.observe(name, cur_print, brand="flashforge")
                broadcaster.notify_from_thread()
            stop_flag.wait(3)  # atualiza a cada 3s

    th = threading.Thread(target=run, daemon=True)
    th.start()
    PRINTERS[name] = {"brand": "flashforge", "stop": stop_flag,
                      "thread": th, "ip": ip, "serial": serial,
                      "check_code": check_code}
    return True


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
                # Diagnóstico opcional: grava os dados brutos para investigar
                # campos (ex.: cor do filamento). Ativado por /api/anycubic/debug.
                if _ANYCUBIC_DEBUG.get("on"):
                    try:
                        with open(_ANYCUBIC_DEBUG["arquivo"], "a", encoding="utf-8") as _f:
                            _f.write(json.dumps({"impressora": name, "data": data},
                                                ensure_ascii=False) + "\n")
                    except Exception:
                        pass
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
                    st["_meta"]["brand"] = "anycubic"
                    # guarda o último bruto (só para diagnóstico da cor, etc.)
                    st["_meta"]["_raw"] = data
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
                    client_id=f"3dwork-{name}-{int(time.time())}",
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            except Exception:
                client = mqtt.Client(client_id=f"3dwork-{name}-{int(time.time())}")
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
    PRINTERS[name] = {"client": holder, "stop": stop_flag, "thread": thread,
                      "brand": "anycubic"}
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
    # Flashforge usa HTTP REST (porta 8898) — adapter próprio.
    if cfg.get("brand") == "flashforge":
        return start_flashforge_printer(cfg)

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
    PRINTERS[name] = {"client": client, "stop": stop_flag, "thread": thread,
                      "brand": "bambu", "serial": serial,
                      "request_topic": request_topic}
    thread.start()


# ---------------------------------------------------------------------------
# Servidor web
# ---------------------------------------------------------------------------
app = FastAPI()


@app.on_event("startup")
async def _startup():
    broadcaster.loop = asyncio.get_event_loop()
    # se o acesso remoto estiver marcado como "sempre ligado", sobe o túnel
    try:
        cfg = _remoto_cfg()
        if cfg.get("auto") and cfg.get("token"):
            asyncio.get_event_loop().run_in_executor(None, _tunnel_start, 8000)
            print("[remoto] acesso remoto automático ativado no início.")
    except Exception as exc:
        print("[remoto] não consegui iniciar o acesso remoto automático:", exc)


ATIVACAO_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ativação · FarmSync</title>
<style>
  *{box-sizing:border-box;margin:0;padding:0}
  input[type=number]{-moz-appearance:textfield; appearance:textfield}
  input[type=number]::-webkit-inner-spin-button,
  input[type=number]::-webkit-outer-spin-button{-webkit-appearance:none; margin:0}
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
    <img class="logo" src="__LOGO_SRC__" alt="FarmSync">
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
  const msg=`Olá! Gostaria de comprar a licença do FarmSync.\n\nCódigo da Máquina: ${fp}`;
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
<title>Entrar · FarmSync</title>
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
    <img class="logo" src="__LOGO_SRC__" alt="FarmSync">
    <h1>FarmSync · Farm de Impressoras</h1>
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
    AUTH["senha_padrao"] = (new_pw == SENHA_PADRAO)   # some o aviso ao trocar
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
                         .replace("__APP_VERSION__", APP_VERSION) \
                         .replace("__SENHA_PADRAO__",
                                  "1" if AUTH.get("senha_padrao") else "0")
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


@app.get("/api/changelog")
async def api_changelog(request: Request):
    if (block := _need_auth(request)):
        return block
    return {"ok": True, "versao_atual": APP_VERSION, "itens": CHANGELOG}


@app.get("/api/licenca/info")
async def api_licenca_info(request: Request):
    if (block := _need_auth(request)):
        return block
    st = LICENSE_STATE or {}
    pacote = ""
    if _CORE_OK and hasattr(_core, "activation_package"):
        try:
            pacote = _core.activation_package()
        except Exception:
            pacote = ""
    return {"ok": st.get("ok", False),
            "fingerprint": st.get("fingerprint", ""),
            "pacote": pacote,                       # inclui hashes p/ tolerância
            "cliente": st.get("cliente", ""),
            "expira_em": st.get("expira_em"),
            "dias_restantes": st.get("dias_restantes")}


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
            # confere se o arquivo publicado é MESMO a versão anunciada.
            # Protege contra o caso em que o versao.json foi atualizado mas o
            # arquivo não (o sistema seria rebaixado e ficaria em laço).
            import re as _re
            m = _re.search(rb'APP_VERSION\s*=\s*"([^"]+)"', novo)
            ver_baixada = m.group(1).decode() if m else ""
            ver_anunciada = str(info.get("nova", ""))
            if ver_baixada != ver_anunciada:
                return {"ok": False,
                        "erro": (f"O arquivo publicado é a versão {ver_baixada or '?'}, "
                                 f"mas foi anunciada a {ver_anunciada}. "
                                 "Atualização cancelada por segurança — "
                                 "nada foi alterado.")}
            if not _version_maior(ver_baixada, APP_VERSION):
                return {"ok": False,
                        "erro": (f"O arquivo publicado ({ver_baixada}) não é mais novo "
                                 f"que o instalado ({APP_VERSION}). "
                                 "Atualização cancelada.")}
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


def _porta_livre(port=8000):
    """Retorna True se a porta 8000 está livre para uso."""
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        s.close()


def _reiniciar_sistema():
    """Relança o sistema (novo processo) e encerra o atual, aguardando a porta
    8000 ser liberada antes de subir a nova instância. Funciona tanto quando o
    sistema roda em janela (python.exe) quanto escondido (pythonw.exe)."""
    import sys
    import subprocess
    import threading

    def _fazer():
        import time
        script = str(Path(__file__).resolve())
        base_dir = Path(__file__).resolve().parent
        args = [a for a in sys.argv[1:] if a != "--init-only"]

        # usa SEMPRE o python.exe (não o pythonw.exe) para o relançador e para
        # subir o app — o pythonw com processo destacado às vezes morre calado
        # no Windows, e era o que fazia o reinício automático falhar quando o
        # sistema tinha sido iniciado escondido (pela inicialização automática).
        exe = sys.executable
        try:
            p = Path(exe)
            cand = p.with_name("python.exe")
            if cand.exists():
                exe = str(cand)
        except Exception:
            pass

        flags = 0
        if platform.system() == "Windows":
            flags = (getattr(subprocess, "DETACHED_PROCESS", 0)
                     | getattr(subprocess, "CREATE_NO_WINDOW", 0))

        # escreve o relançador como um arquivo .py de verdade (mais confiável
        # que passar código por -c, especialmente com processo destacado)
        relauncher_py = base_dir / "_relaunch.py"
        codigo = (
            "import time, socket, subprocess, os\n"
            "def livre():\n"
            "    s = socket.socket()\n"
            "    try:\n"
            "        s.bind(('127.0.0.1', 8000)); return True\n"
            "    except OSError:\n"
            "        return False\n"
            "    finally:\n"
            "        s.close()\n"
            "for _ in range(120):\n"       # espera até ~60s a porta liberar
            "    if livre(): break\n"
            "    time.sleep(0.5)\n"
            f"subprocess.Popen([{exe!r}, {script!r}] + {args!r})\n"
            "try:\n"
            f"    os.remove({str(relauncher_py)!r})\n"   # se auto-apaga
            "except Exception:\n"
            "    pass\n"
        )
        try:
            relauncher_py.write_text(codigo, encoding="utf-8")
        except Exception as exc:
            print("[update] falha ao escrever o relançador:", exc)
            return
        try:
            subprocess.Popen([exe, str(relauncher_py)], close_fds=True,
                             creationflags=flags)
        except Exception as exc:
            print("[update] falha ao iniciar o relançador:", exc)
            return
        time.sleep(1.2)
        print("[update] reiniciando para aplicar a nova versão…")
        os._exit(0)

    threading.Thread(target=_fazer, daemon=True).start()


@app.post("/api/update/reiniciar")
async def api_update_reiniciar(request: Request):
    if (block := _need_auth(request)):
        return block
    _reiniciar_sistema()
    return {"ok": True}


@app.get("/api/health")
async def api_health():
    """Verificação simples de que o sistema está no ar. Sem login, para o
    navegador conseguir detectar quando o sistema volta após reiniciar."""
    return {"ok": True, "versao": APP_VERSION}


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
# ORÇAMENTOS — cria, salva, lista e exporta em PDF.
# Usa os parâmetros da Calculadora para precificar cada peça.
# ---------------------------------------------------------------------------
ORC_PATH = Path(__file__).with_name("orcamentos.json")
ORC_LOCK = threading.Lock()

ORC_STATUS = ("rascunho", "enviado", "aprovado", "recusado")


def _orc_load():
    try:
        if ORC_PATH.exists():
            data = json.loads(ORC_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
    except Exception as exc:
        print("[orcamentos] arquivo inválido:", exc)
    return []


def _orc_save_all(lista):
    try:
        ORC_PATH.write_text(json.dumps(lista, ensure_ascii=False, indent=2),
                            encoding="utf-8")
        return True
    except Exception as exc:
        print("[orcamentos] falha ao salvar:", exc)
        return False


def _orc_proximo_numero(lista):
    maior = 0
    for o in lista:
        try:
            n = int(str(o.get("numero", "0")).split("-")[-1])
            maior = max(maior, n)
        except Exception:
            pass
    return f"ORC-{maior + 1:04d}"


def _f(v, padrao=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return padrao


def _orc_data_iso(data_str, anterior=None):
    """Converte a data 'YYYY-MM-DD' vinda do editor para ISO, preservando a
    hora original do orçamento (se houver). Retorna None se a data for vazia
    ou inválida — nesse caso o chamador mantém o valor que já tinha."""
    data_str = (data_str or "").strip()
    if not data_str:
        return None
    try:
        d = datetime.strptime(data_str[:10], "%Y-%m-%d").date()
    except ValueError:
        return None
    # tenta preservar a hora do 'criado_em' anterior
    hora = None
    if anterior:
        try:
            hora = datetime.fromisoformat(str(anterior)).time()
        except ValueError:
            hora = None
    if hora is None:
        hora = datetime.now().time()
    return datetime.combine(d, hora).isoformat(timespec="seconds")


def orc_calcular(orc, cfg=None):
    """Calcula os totais do orçamento.

    O 'valor de venda' de cada peça é digitado pelo dono (é o que o cliente
    paga). O sistema calcula à parte o CUSTO real (filamento informado +
    energia + máquina) apenas para o dono acompanhar a margem — isso NÃO vai
    para o PDF do cliente."""
    cfg = cfg or load_calc_cfg()
    potencia_w = _f(cfg.get("potencia_w"), 150.0)
    preco_kwh = _f(cfg.get("preco_kwh"), 0.95)
    valor_maq = _f(cfg.get("valor_maquina"), 0.0)
    vida_h = _f(cfg.get("vida_util_h"), 0.0)

    itens_out = []
    soma_venda = 0.0      # o que o cliente paga (soma dos valores de venda)
    soma_custo = 0.0      # custo real total (só para o dono)
    total_peso = 0.0
    total_horas = 0.0

    for it in orc.get("itens", []):
        qtd = max(1, int(_f(it.get("qtd"), 1)))
        # peso em gramas (padrão) ou kilos
        peso_val = _f(it.get("peso_g"))
        peso_g = peso_val * 1000.0 if (it.get("peso_unid") or "g") == "kg" else peso_val
        # tempo em minutos (padrão) ou horas
        tempo_val = _f(it.get("tempo_min"))
        minutos = tempo_val * 60.0 if (it.get("tempo_unid") or "min") == "h" else tempo_val
        # custo do filamento: o dono informa o preço do ROLO (R$/kg) e o
        # sistema calcula o custo desta peça pelo peso.
        preco_rolo_kg = _f(it.get("valor_filamento"))   # R$ por kg (preço do rolo)
        c_material = (peso_g / 1000.0) * preco_rolo_kg
        # valor de venda por unidade: digitado pelo dono
        venda_unit = _f(it.get("valor_venda"))

        c_energia = (minutos / 60.0) * (potencia_w / 1000.0) * preco_kwh
        c_maquina = (minutos / 60.0) * (valor_maq / vida_h) if vida_h > 0 else 0.0
        custo_unit = c_material + c_energia + c_maquina   # custo real por unidade

        subtotal = venda_unit * qtd                       # o cliente paga isto
        custo_total_item = custo_unit * qtd
        lucro_item = subtotal - custo_total_item

        soma_venda += subtotal
        soma_custo += custo_total_item
        total_peso += peso_g * qtd
        total_horas += (minutos / 60.0) * qtd

        itens_out.append({
            "custo_material": round(c_material, 2),
            "custo_energia": round(c_energia, 2),
            "custo_maquina": round(c_maquina, 2),
            "custo_unit": round(custo_unit, 2),      # custo real por unidade
            "preco_unit": round(venda_unit, 2),      # valor de venda por unidade
            "subtotal": round(subtotal, 2),          # total da linha (venda)
            "lucro": round(lucro_item, 2),           # margem da linha (só dono)
        })

    extras = sum(_f(e.get("valor")) for e in orc.get("extras", []))
    bruto = soma_venda + extras

    desc_pct = _f(orc.get("desconto_pct"))
    desc_rs = _f(orc.get("desconto_rs"))
    desconto = bruto * (desc_pct / 100.0) + desc_rs
    total = max(0.0, bruto - desconto)

    lucro_total = total - soma_custo   # margem final (só para o dono)
    margem_pct = (lucro_total / total * 100.0) if total > 0 else 0.0

    return {
        "itens": itens_out,
        "soma_itens": round(soma_venda, 2),
        "custo_total": round(soma_custo, 2),      # custo real (só dono)
        "lucro_total": round(lucro_total, 2),     # margem em R$ (só dono)
        "margem_pct": round(margem_pct, 1),       # margem em % (só dono)
        "extras": round(extras, 2),
        "bruto": round(bruto, 2),
        "desconto": round(desconto, 2),
        "total": round(total, 2),
        "peso_total_g": round(total_peso, 1),
        "horas_total": round(total_horas, 1),
    }


@app.get("/api/orcamentos/list")
async def api_orc_list(request: Request):
    if (block := _need_auth(request)):
        return block
    lista = await asyncio.to_thread(_orc_load)
    # devolve resumido (sem os itens) para a listagem ficar leve
    resumo = []
    for o in sorted(lista, key=lambda x: x.get("criado_em", ""), reverse=True):
        resumo.append({
            "id": o.get("id"),
            "numero": o.get("numero"),
            "cliente": (o.get("cliente") or {}).get("nome", ""),
            "criado_em": o.get("criado_em"),
            "validade_dias": o.get("validade_dias", 7),
            "status": o.get("status", "rascunho"),
            "total": (o.get("totais") or {}).get("total", 0),
            "qtd_itens": len(o.get("itens", [])),
        })
    return {"ok": True, "orcamentos": resumo}


@app.get("/api/orcamentos/get")
async def api_orc_get(request: Request):
    if (block := _need_auth(request)):
        return block
    oid = request.query_params.get("id", "")
    lista = await asyncio.to_thread(_orc_load)
    o = next((x for x in lista if x.get("id") == oid), None)
    if not o:
        return {"ok": False, "error": "Orçamento não encontrado."}
    return {"ok": True, "orcamento": o, "cfg": load_calc_cfg()}


@app.post("/api/orcamentos/preview")
async def api_orc_preview(request: Request):
    """Calcula os totais sem salvar (usado enquanto o usuário digita)."""
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    return {"ok": True, "totais": orc_calcular(body)}


@app.post("/api/orcamentos/save")
async def api_orc_save(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    with ORC_LOCK:
        lista = _orc_load()
        oid = body.get("id")
        agora = datetime.now().isoformat(timespec="seconds")
        totais = orc_calcular(body)
        if oid:
            idx = next((i for i, x in enumerate(lista) if x.get("id") == oid), None)
            if idx is None:
                return {"ok": False, "error": "Orçamento não encontrado."}
            orc = lista[idx]
            orc.update({
                "cliente": body.get("cliente", {}),
                "itens": body.get("itens", []),
                "extras": body.get("extras", []),
                "desconto_pct": _f(body.get("desconto_pct")),
                "desconto_rs": _f(body.get("desconto_rs")),
                "obs": body.get("obs", ""),
                "validade_dias": int(_f(body.get("validade_dias"), 7)),
                "status": body.get("status", orc.get("status", "rascunho")),
                "atualizado_em": agora,
                "totais": totais,
            })
            # data escolhida pelo usuário (mantém a hora original, troca só o dia)
            _dt = _orc_data_iso(body.get("data_orc"), orc.get("criado_em"))
            if _dt:
                orc["criado_em"] = _dt
            lista[idx] = orc
        else:
            oid = f"{int(time.time() * 1000)}"
            orc = {
                "id": oid,
                "numero": _orc_proximo_numero(lista),
                "criado_em": _orc_data_iso(body.get("data_orc"), agora) or agora,
                "atualizado_em": agora,
                "cliente": body.get("cliente", {}),
                "itens": body.get("itens", []),
                "extras": body.get("extras", []),
                "desconto_pct": _f(body.get("desconto_pct")),
                "desconto_rs": _f(body.get("desconto_rs")),
                "obs": body.get("obs", ""),
                "validade_dias": int(_f(body.get("validade_dias"), 7)),
                "status": body.get("status", "rascunho"),
                "totais": totais,
            }
            lista.append(orc)
        if not _orc_save_all(lista):
            return {"ok": False, "error": "Não foi possível salvar."}
    return {"ok": True, "id": oid, "numero": orc.get("numero"), "totais": totais}


@app.post("/api/orcamentos/status")
async def api_orc_status(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    oid, novo = body.get("id"), body.get("status")
    if novo not in ORC_STATUS:
        return {"ok": False, "error": "Situação inválida."}
    with ORC_LOCK:
        lista = _orc_load()
        o = next((x for x in lista if x.get("id") == oid), None)
        if not o:
            return {"ok": False, "error": "Orçamento não encontrado."}
        o["status"] = novo
        o["atualizado_em"] = datetime.now().isoformat(timespec="seconds")
        _orc_save_all(lista)
    return {"ok": True}


@app.post("/api/orcamentos/delete")
async def api_orc_delete(request: Request):
    if (block := _need_auth(request)):
        return block
    oid = (await request.json()).get("id")
    with ORC_LOCK:
        lista = _orc_load()
        nova = [x for x in lista if x.get("id") != oid]
        if len(nova) == len(lista):
            return {"ok": False, "error": "Orçamento não encontrado."}
        _orc_save_all(nova)
    return {"ok": True}


@app.post("/api/orcamentos/duplicar")
async def api_orc_duplicar(request: Request):
    if (block := _need_auth(request)):
        return block
    oid = (await request.json()).get("id")
    with ORC_LOCK:
        lista = _orc_load()
        o = next((x for x in lista if x.get("id") == oid), None)
        if not o:
            return {"ok": False, "error": "Orçamento não encontrado."}
        agora = datetime.now().isoformat(timespec="seconds")
        novo = dict(o)
        novo["id"] = f"{int(time.time() * 1000)}"
        novo["numero"] = _orc_proximo_numero(lista)
        novo["criado_em"] = agora
        novo["atualizado_em"] = agora
        novo["status"] = "rascunho"
        lista.append(novo)
        _orc_save_all(lista)
    return {"ok": True, "id": novo["id"]}


def _orc_pdf(orc):
    """Gera o PDF do orçamento para enviar ao cliente."""
    import io
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                        Paragraph, Spacer, Image)
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    except ImportError:
        return None

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=18 * mm,
                            bottomMargin=18 * mm, leftMargin=16 * mm,
                            rightMargin=16 * mm,
                            title=f"Orçamento {orc.get('numero','')}")
    ss = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", parent=ss["Heading1"], fontSize=17, spaceAfter=2)
    small = ParagraphStyle("small", parent=ss["Normal"], fontSize=8.5,
                           textColor=colors.HexColor("#666666"))
    normal = ss["Normal"]
    el = []

    cli = orc.get("cliente") or {}
    tot = orc.get("totais") or {}

    # Logo da empresa do cliente, SÓ se ele trocou pelo próprio.
    # Se ainda usa o padrão 3DWORK, não aparece logo nenhum no orçamento.
    try:
        logo_bytes, _fmt = get_logo_custom_bytes()
        if logo_bytes:
            from reportlab.lib.utils import ImageReader
            ir = ImageReader(io.BytesIO(logo_bytes))
            iw, ih = ir.getSize()
            alvo_h = 16 * mm
            alvo_w = alvo_h * (iw / ih) if ih else 40 * mm
            max_w = 60 * mm
            if alvo_w > max_w:
                alvo_w = max_w
                alvo_h = alvo_w * (ih / iw) if iw else alvo_h
            img = Image(io.BytesIO(logo_bytes), width=alvo_w, height=alvo_h)
            img.hAlign = "LEFT"
            el.append(img)
            el.append(Spacer(1, 5 * mm))
    except Exception as exc:
        print("[orcamento] não consegui inserir o logo:", exc)

    el.append(Paragraph(f"Orçamento {orc.get('numero','')}", h1))
    criado = str(orc.get("criado_em", ""))[:10]
    try:
        criado = datetime.fromisoformat(orc["criado_em"]).strftime("%d/%m/%Y")
    except Exception:
        pass
    val = int(_f(orc.get("validade_dias"), 7))
    el.append(Paragraph(f"Emitido em {criado} · Válido por {val} dias", small))
    el.append(Spacer(1, 8 * mm))

    # Dados do cliente
    dados = []
    if cli.get("nome"):
        dados.append(["Cliente:", cli.get("nome", "")])
    if cli.get("contato"):
        dados.append(["Contato:", cli.get("contato", "")])
    if cli.get("email"):
        dados.append(["E-mail:", cli.get("email", "")])
    if dados:
        t = Table(dados, colWidths=[22 * mm, None])
        t.setStyle(TableStyle([
            ("FONTSIZE", (0, 0), (-1, -1), 9.5),
            ("TEXTCOLOR", (0, 0), (0, -1), colors.HexColor("#666666")),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 1),
        ]))
        el.append(t)
        el.append(Spacer(1, 6 * mm))

    # Itens
    linhas = [["#", "Descrição", "Material", "Qtd", "Unitário", "Subtotal"]]
    calc_itens = tot.get("itens", [])
    for i, it in enumerate(orc.get("itens", [])):
        ci = calc_itens[i] if i < len(calc_itens) else {}
        qtd = int(_f(it.get("qtd"), 1))
        linhas.append([
            str(i + 1),
            Paragraph(str(it.get("desc", "") or "—"), normal),
            it.get("material", "") or "—",
            str(qtd),
            f"R$ {_f(ci.get('preco_unit')):.2f}".replace(".", ","),
            f"R$ {_f(ci.get('subtotal')):.2f}".replace(".", ","),
        ])
    t = Table(linhas, colWidths=[8 * mm, None, 22 * mm, 12 * mm, 25 * mm, 25 * mm],
              repeatRows=1)
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1a2333")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (3, 0), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1),
         [colors.white, colors.HexColor("#f4f6f9")]),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#dfe4ea")),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    el.append(t)
    el.append(Spacer(1, 5 * mm))

    # Extras
    extras = orc.get("extras", [])
    if extras:
        le = [["Serviços adicionais", "Valor"]]
        for e in extras:
            le.append([e.get("desc", "") or "—",
                       f"R$ {_f(e.get('valor')):.2f}".replace(".", ",")])
        te = Table(le, colWidths=[None, 30 * mm])
        te.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#eef1f5")),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#dfe4ea")),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        el.append(te)
        el.append(Spacer(1, 5 * mm))

    # Totais
    lt = []
    if _f(tot.get("extras")) or _f(tot.get("desconto")):
        lt.append(["Subtotal", f"R$ {_f(tot.get('bruto')):.2f}".replace(".", ",")])
    if _f(tot.get("desconto")):
        lt.append(["Desconto",
                   f"- R$ {_f(tot.get('desconto')):.2f}".replace(".", ",")])
    lt.append(["TOTAL", f"R$ {_f(tot.get('total')):.2f}".replace(".", ",")])
    tt = Table(lt, colWidths=[None, 35 * mm], hAlign="RIGHT")
    tt.setStyle(TableStyle([
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("ALIGN", (0, 0), (-1, -1), "RIGHT"),
        ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, -1), (-1, -1), 13),
        ("TEXTCOLOR", (0, -1), (-1, -1), colors.HexColor("#0a7a3f")),
        ("LINEABOVE", (0, -1), (-1, -1), 0.8, colors.HexColor("#333333")),
        ("TOPPADDING", (0, -1), (-1, -1), 5),
    ]))
    el.append(tt)

    if orc.get("obs"):
        el.append(Spacer(1, 7 * mm))
        el.append(Paragraph("<b>Observações</b>", normal))
        el.append(Spacer(1, 1.5 * mm))
        el.append(Paragraph(str(orc["obs"]).replace("\n", "<br/>"), small))

    doc.build(el)
    buf.seek(0)
    return buf.read()


@app.get("/api/orcamentos/pdf")
async def api_orc_pdf(request: Request):
    if (block := _need_auth(request)):
        return block
    oid = request.query_params.get("id", "")
    lista = await asyncio.to_thread(_orc_load)
    o = next((x for x in lista if x.get("id") == oid), None)
    if not o:
        return JSONResponse({"ok": False, "error": "Orçamento não encontrado."},
                            status_code=404)
    pdf = await asyncio.to_thread(_orc_pdf, o)
    if pdf is None:
        return JSONResponse({"ok": False, "error": "reportlab não instalado."},
                            status_code=500)
    from fastapi.responses import Response
    nome = f"orcamento_{o.get('numero','')}.pdf"
    return Response(pdf, media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="{nome}"'})


# ---------------------------------------------------------------------------
# Câmera — relay de vídeo para o navegador (via ffmpeg)
#   Kobra:     FLV em http://IP:18088/live/<token> (URL vem no report MQTT).
#   Bambu A1:  protocolo chamber_image (TCP+TLS na porta 6000).
#   ffmpeg converte ambos em MJPEG, que o navegador exibe num <img>.
# ---------------------------------------------------------------------------
_FFMPEG_PATH = None


def _no_window_flags():
    """Retorna os flags para o subprocess NÃO abrir janela do CMD no Windows.
    Usado em todas as chamadas do ffmpeg, para o cliente não ver aquela
    janela preta piscando quando abre a câmera."""
    if platform.system() == "Windows":
        return getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return 0


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


# ---------------------------------------------------------------------------
# Acesso remoto via Cloudflare Tunnel (sem abrir porta no roteador).
# Um pequeno programa (cloudflared) cria um túnel de saída até a Cloudflare
# e devolve um endereço https público. O login continua sendo exigido.
# ---------------------------------------------------------------------------
_TUNNEL = {"proc": None, "url": None, "status": "desligado", "erro": None}
_CLOUDFLARED_PATH = None
REMOTO_CONFIG_PATH = Path(__file__).with_name("remoto_config.json")


def _remoto_cfg():
    """Lê a configuração do acesso remoto (token e endereço fixo do cliente)."""
    try:
        if REMOTO_CONFIG_PATH.exists():
            return json.loads(REMOTO_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"token": "", "hostname": "", "auto": False}


def _remoto_save_cfg(cfg):
    try:
        REMOTO_CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2),
                                      encoding="utf-8")
        return True
    except Exception as exc:
        print("[remoto] falha ao salvar config:", exc)
        return False


def _find_cloudflared():
    """Localiza o cloudflared: PATH ou pasta do sistema."""
    global _CLOUDFLARED_PATH
    if _CLOUDFLARED_PATH:
        return _CLOUDFLARED_PATH
    import shutil
    exe = shutil.which("cloudflared")
    if exe:
        _CLOUDFLARED_PATH = exe
        return exe
    here = Path(__file__).parent
    for cand in [here / "cloudflared.exe", here / "cloudflared" / "cloudflared.exe",
                 here / "cloudflared"]:
        if cand.exists():
            _CLOUDFLARED_PATH = str(cand)
            return _CLOUDFLARED_PATH
    return None


def _download_cloudflared():
    """Baixa o cloudflared para Windows (uma vez), na pasta do sistema."""
    if not requests:
        return None
    if platform.system() != "Windows":
        return None
    destino = Path(__file__).parent / "cloudflared.exe"
    if destino.exists():
        return str(destino)
    url = ("https://github.com/cloudflare/cloudflared/releases/latest/"
           "download/cloudflared-windows-amd64.exe")
    try:
        print("[remoto] baixando cloudflared (uma única vez)…")
        r = requests.get(url, timeout=120, stream=True)
        r.raise_for_status()
        with open(destino, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)
        print("[remoto] cloudflared baixado.")
        return str(destino)
    except Exception as exc:
        print("[remoto] falha ao baixar cloudflared:", exc)
        try:
            destino.unlink(missing_ok=True)
        except Exception:
            pass
        return None


def _tunnel_start(port=8000):
    """Inicia o túnel. Se houver token configurado, sobe no endereço FIXO do
    cliente (túnel nomeado). Senão, cria um link temporário (trycloudflare)."""
    import subprocess
    import threading as _th
    if _TUNNEL["proc"] and _TUNNEL["proc"].poll() is None:
        return _TUNNEL  # já rodando

    exe = _find_cloudflared() or _download_cloudflared()
    if not exe:
        _TUNNEL.update(status="erro",
                       erro="Não encontrei o cloudflared. Verifique a instalação "
                            "ou a conexão com a internet.")
        return _TUNNEL

    cfg = _remoto_cfg()
    token = (cfg.get("token") or "").strip()
    hostname = (cfg.get("hostname") or "").strip()
    nomeado = bool(token)

    _TUNNEL.update(status="iniciando", url=None, erro=None)
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if platform.system() == "Windows" else 0

    if nomeado:
        # túnel nomeado: endereço fixo definido na conta Cloudflare
        cmd = [exe, "tunnel", "--no-autoupdate", "run", "--token", token]
    else:
        # link temporário (muda a cada vez) — modo sem configuração
        cmd = [exe, "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"]

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1, creationflags=flags)
    except Exception as exc:
        _TUNNEL.update(status="erro", erro=f"Não consegui iniciar o túnel: {exc}")
        return _TUNNEL
    _TUNNEL["proc"] = proc

    def _ler_saida():
        import re
        padrao_tmp = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com")
        conectou = False
        for linha in proc.stdout:
            if nomeado:
                # o túnel nomeado não imprime a URL; detecta a conexão
                if ("Registered tunnel connection" in linha
                        or "Connection registered" in linha) and not conectou:
                    conectou = True
                    url = hostname
                    if url and not url.startswith("http"):
                        url = "https://" + url
                    _TUNNEL.update(url=url, status="ligado", erro=None)
                    print(f"[remoto] acesso remoto ativo (fixo): {url}")
            else:
                m = padrao_tmp.search(linha)
                if m and not _TUNNEL["url"]:
                    _TUNNEL.update(url=m.group(0), status="ligado", erro=None)
                    print(f"[remoto] acesso remoto ativo: {_TUNNEL['url']}")
        if _TUNNEL["status"] != "desligado":
            _TUNNEL.update(status="desligado", url=None)

    _th.Thread(target=_ler_saida, daemon=True).start()
    return _TUNNEL


def _tunnel_stop():
    proc = _TUNNEL.get("proc")
    if proc and proc.poll() is None:
        try:
            proc.terminate()
        except Exception:
            pass
    _TUNNEL.update(proc=None, url=None, status="desligado", erro=None)
    return _TUNNEL


@app.get("/api/remoto/status")
async def api_remoto_status(request: Request):
    if (block := _need_auth(request)):
        return block
    cfg = _remoto_cfg()
    return {"ok": True, "status": _TUNNEL["status"], "url": _TUNNEL["url"],
            "erro": _TUNNEL["erro"],
            "fixo": bool(cfg.get("token")),
            "hostname": cfg.get("hostname", ""),
            "auto": bool(cfg.get("auto"))}


@app.get("/api/remoto/config")
async def api_remoto_config(request: Request):
    if (block := _need_auth(request)):
        return block
    cfg = _remoto_cfg()
    # não devolve o token inteiro por segurança, só se está configurado
    return {"ok": True, "tem_token": bool(cfg.get("token")),
            "hostname": cfg.get("hostname", ""), "auto": bool(cfg.get("auto"))}


@app.post("/api/remoto/config")
async def api_remoto_config_save(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    cfg = _remoto_cfg()
    if "token" in body:
        cfg["token"] = (body.get("token") or "").strip()
    if "hostname" in body:
        h = (body.get("hostname") or "").strip()
        h = h.replace("https://", "").replace("http://", "").strip("/")
        cfg["hostname"] = h
    if "auto" in body:
        cfg["auto"] = bool(body.get("auto"))
    if not _remoto_save_cfg(cfg):
        return {"ok": False, "error": "Não foi possível salvar."}
    return {"ok": True, "tem_token": bool(cfg.get("token")),
            "hostname": cfg.get("hostname", ""), "auto": bool(cfg.get("auto"))}


@app.post("/api/remoto/ligar")
async def api_remoto_ligar(request: Request):
    if (block := _need_auth(request)):
        return block
    await asyncio.to_thread(_tunnel_start, 8000)
    for _ in range(40):
        if _TUNNEL["url"] or _TUNNEL["status"] == "erro":
            break
        await asyncio.sleep(0.5)
    return {"ok": _TUNNEL["status"] == "ligado", "status": _TUNNEL["status"],
            "url": _TUNNEL["url"], "erro": _TUNNEL["erro"]}


@app.post("/api/remoto/desligar")
async def api_remoto_desligar(request: Request):
    if (block := _need_auth(request)):
        return block
    await asyncio.to_thread(_tunnel_stop)
    return {"ok": True, "status": "desligado"}





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
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                creationflags=_no_window_flags())
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
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            creationflags=_no_window_flags())
        feeder = threading.Thread(target=_feed_bambu_jpegs,
                                  args=(proc, bambu_ip, bambu_code), daemon=True)
        feeder.start()
    else:
        # Kobra: ffmpeg lê o FLV direto da URL
        proc = subprocess.Popen(
            [ffmpeg, "-fflags", "nobuffer", "-flags", "low_delay",
             "-i", input_url_or_args,
             "-f", "mpjpeg", "-q:v", "5", "-r", "10", "pipe:1"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            creationflags=_no_window_flags())

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
                                        Paragraph, Spacer, Image)
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
    # Logo do cliente (ou 3DWORK padrão) no topo do relatório
    try:
        logo_bytes, _fmt = get_logo_bytes()
        if logo_bytes:
            from reportlab.lib.utils import ImageReader
            ir = ImageReader(io.BytesIO(logo_bytes))
            iw, ih = ir.getSize()
            alvo_h = 16 * mm
            alvo_w = alvo_h * (iw / ih) if ih else 40 * mm
            max_w = 60 * mm
            if alvo_w > max_w:
                alvo_w = max_w
                alvo_h = alvo_w * (ih / iw) if iw else alvo_h
            img = Image(io.BytesIO(logo_bytes), width=alvo_w, height=alvo_h)
            img.hAlign = "LEFT"
            elems.append(img)
            elems.append(Spacer(1, 5 * mm))
    except Exception as exc:
        print("[relatorio] não consegui inserir o logo:", exc)

    elems.append(Paragraph("Relatório de Impressões", title))
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
# A API da Bambu recusa clientes com identificação incomum, por isso usamos
# cabeçalhos equivalentes aos de um navegador.
BAMBU_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://bambulab.com",
    "Referer": "https://bambulab.com/",
}


def _bambu_erro_msg(r):
    """Extrai a explicação que a Bambu manda junto com o erro."""
    try:
        d = r.json()
    except Exception:
        texto = (r.text or "").strip()
        return texto[:200] if texto else ""
    if isinstance(d, dict):
        for chave in ("message", "error", "msg", "errorMsg", "error_description"):
            v = d.get(chave)
            if isinstance(v, str) and v.strip():
                return v.strip()[:200]
        code = d.get("code")
        if code:
            return f"código {code}"
    return ""

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


def _bambu_send_code(email, region):
    """Pede à Bambu que envie o código de verificação para o e-mail.
    Sem esta chamada o código nunca chega ao usuário."""
    if not requests:
        return False, "Dependência 'requests' não instalada."
    base = BAMBU_API.get(region, BAMBU_API["us"])
    url = f"{base}/v1/user-service/user/sendemail/code"
    try:
        r = requests.post(url, json={"email": email, "type": "codeLogin"},
                          headers=BAMBU_HEADERS, timeout=20)
    except Exception as exc:
        return False, f"Erro de conexão ao pedir o código: {exc}"
    if r.status_code >= 400:
        return False, ("Não consegui enviar o código de verificação "
                       f"(erro {r.status_code}).")
    return True, None


def _bambu_tfa(tfa_key, tfa_code):
    """Conclui o login de contas com autenticador (2FA).
    O token volta nos cookies da resposta."""
    if not requests:
        return {"error": "Dependência 'requests' não instalada."}
    try:
        r = requests.post("https://bambulab.com/api/sign-in/tfa",
                          json={"tfaKey": tfa_key, "tfaCode": tfa_code},
                          headers=BAMBU_HEADERS, timeout=20)
    except Exception as exc:
        return {"error": f"Erro de conexão: {exc}"}
    if r.status_code >= 400:
        return {"error": "Código do autenticador inválido ou expirado."}
    token = r.cookies.get("token")
    if not token:
        # algumas respostas trazem o token no corpo
        try:
            d = r.json()
            token = d.get("accessToken") or d.get("token")
        except Exception:
            token = None
    if not token:
        return {"error": "Não recebi o token após a verificação em duas etapas."}
    return {"token": token}


def _bambu_login(email, password, region, code=None, tfa_key=None, tfa_code=None):
    """Login na conta Bambu. Trata os três caminhos possíveis:
    senha direta, código enviado por e-mail e autenticador (2FA)."""
    if not requests:
        return {"error": "Dependência 'requests' não instalada no servidor."}

    # etapa final do autenticador
    if tfa_key and tfa_code:
        return _bambu_tfa(tfa_key, tfa_code)

    base = BAMBU_API.get(region, BAMBU_API["us"])
    url = f"{base}/v1/user-service/user/login"
    try:
        if code:
            corpo = {"account": email, "code": code}
        else:
            corpo = {"account": email, "password": password, "apiError": ""}
        r = requests.post(url, json=corpo, headers=BAMBU_HEADERS, timeout=20)
    except Exception as exc:
        return {"error": f"Erro de conexão com a Bambu: {exc}"}

    detalhe = _bambu_erro_msg(r) if r.status_code >= 400 else ""
    if r.status_code >= 400:
        print(f"[bambu] login recusado {r.status_code}: {detalhe or r.text[:300]}")
    if r.status_code in (401, 403):
        return {"error": "E-mail ou senha incorretos."
                         + (f" (Bambu: {detalhe})" if detalhe else "")}
    if r.status_code == 400:
        # 400 costuma ser dado mal formatado ou conta que exige verificação
        baixo = (detalhe or "").lower()
        if "password" in baixo or "account" in baixo or "credential" in baixo:
            return {"error": f"E-mail ou senha incorretos. (Bambu: {detalhe})"}
        if code:
            return {"error": f"Código incorreto ou expirado. (Bambu: {detalhe})"
                    if detalhe else "Código incorreto ou expirado."}
        return {"error": ("A Bambu recusou o login."
                          + (f" Resposta: {detalhe}" if detalhe else
                             " Confira o e-mail e a senha e tente de novo."))}
    if r.status_code >= 400:
        return {"error": (f"A Bambu recusou o login (erro {r.status_code})."
                          + (f" {detalhe}" if detalhe else ""))}

    try:
        d = r.json()
    except Exception:
        return {"error": "Resposta inesperada da Bambu."}

    login_type = (d.get("loginType") or "").strip()
    token = d.get("accessToken")

    # 1) deu certo direto
    if token and not login_type:
        return {"token": token}

    # 2) conta com autenticador (app de 2 etapas)
    if d.get("tfaKey"):
        return {"need_tfa": True, "tfa_key": d["tfaKey"]}

    # 3) a Bambu quer um código enviado por e-mail
    if login_type in ("verifyCode", "code") or not token:
        if code:
            # já mandamos um código e ainda assim não passou
            return {"error": "Código incorreto ou expirado. Peça um novo."}
        ok, erro = _bambu_send_code(email, region)
        if not ok:
            return {"error": erro}
        return {"need_code": True}

    return {"error": "Não consegui concluir o login. Tente novamente."}


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
        "User-Agent": "3dwork/1.0",
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
        # conta sem senha (criada via Google/Apple/etc): envia o código direto
        if body.get("solicitar_codigo") and not body.get("code"):
            email = (body.get("email") or "").strip()
            if not email:
                return {"ok": False, "error": "Informe o e-mail."}
            ok, erro = _bambu_send_code(email, region)
            if not ok:
                return {"ok": False, "error": erro}
            return {"ok": False, "need_code": True,
                    "mensagem": "Enviamos um código para o seu e-mail. "
                                "Digite-o abaixo."}
        res = _bambu_login(body.get("email", ""), body.get("password", ""),
                           region, body.get("code"),
                           body.get("tfa_key"), body.get("tfa_code"))
        if res.get("error"):
            return {"ok": False, "error": res["error"]}
        if res.get("need_code"):
            return {"ok": False, "need_code": True,
                    "mensagem": "Enviamos um código para o seu e-mail. "
                                "Digite-o abaixo."}
        if res.get("need_tfa"):
            return {"ok": False, "need_tfa": True, "tfa_key": res["tfa_key"],
                    "mensagem": "Sua conta usa verificação em duas etapas. "
                                "Digite o código do aplicativo autenticador."}
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


def _serial_por_ssdp(ip, timeout=5):
    """Tenta descobrir o número de série ouvindo os anúncios da impressora."""
    achadas, _erro = _bambu_ssdp_scan(timeout)
    for p in achadas:
        if p.get("ip") == ip and p.get("serial"):
            return p["serial"]
    return None


def _serial_por_mqtt(ip, access_code, timeout=12):
    """Descobre o número de série conectando na impressora e escutando
    todos os canais (device/+/report). O nome do canal traz o série."""
    import socket as _sk
    encontrado = {"serial": None, "recusado": False}
    pronto = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            client.subscribe("device/+/report")
        else:
            if "not authorized" in str(reason_code).lower():
                encontrado["recusado"] = True
            pronto.set()

    def on_message(client, userdata, msg):
        partes = str(msg.topic).split("/")
        if len(partes) >= 3 and partes[0] == "device" and partes[1]:
            encontrado["serial"] = partes[1]
            pronto.set()

    try:
        client = mqtt.Client(client_id=f"3dwork-busca-{int(time.time())}",
                             callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    except Exception:
        client = mqtt.Client(client_id=f"3dwork-busca-{int(time.time())}")
    client.username_pw_set("bblp", access_code)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(ip, 8883, keepalive=20)
    except Exception:
        return None, "inalcancavel"
    client.loop_start()
    pronto.wait(timeout)
    client.loop_stop()
    try:
        client.disconnect()
    except Exception:
        pass
    if encontrado["recusado"]:
        return None, "codigo_incorreto"
    return encontrado["serial"], None


def descobrir_serial(ip, access_code):
    """Descobre o número de série da impressora só com o IP.
    Devolve (serial, erro)."""
    s = _serial_por_ssdp(ip, timeout=5)
    if s:
        return s, None
    return _serial_por_mqtt(ip, access_code, timeout=12)


@app.post("/api/bambu/reenviar_codigo")
async def api_bambu_reenviar_codigo(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    email = (body.get("email") or "").strip()
    region = body.get("region", "us")
    if not email:
        return {"ok": False, "error": "Informe o e-mail."}
    ok, erro = await asyncio.to_thread(_bambu_send_code, email, region)
    return {"ok": ok, "error": erro}


@app.post("/api/bambu/descobrir_serial")
async def api_bambu_descobrir_serial(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    ip = (body.get("ip") or "").strip()
    code = (body.get("access_code") or "").strip()
    if not ip or not code:
        return {"ok": False, "error": "Informe o IP e o Código de Acesso."}
    serial, erro = await asyncio.to_thread(descobrir_serial, ip, code)
    if serial:
        return {"ok": True, "serial": serial}
    msgs = {
        "codigo_incorreto": ("A impressora recusou o Código de Acesso. "
                             "Confira o código na tela dela."),
        "inalcancavel": ("Não consegui alcançar a impressora nesse IP. "
                         "Confira se ela está ligada e na mesma rede."),
    }
    return {"ok": False, "error": msgs.get(erro,
            "Não consegui descobrir o número de série. "
            "Digite-o manualmente (fica em Configurações → Sobre, na impressora).")}


def _bambu_ssdp_scan(timeout=6):
    """Escuta os anúncios que as impressoras Bambu enviam na rede local
    (SSDP na porta 2021) e devolve o que encontrar: IP, número de série,
    modelo e nome. Assim o usuário não precisa digitar o número de série."""
    import socket
    import struct
    achadas = {}
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        except Exception:
            pass
        sock.bind(("0.0.0.0", 2021))
        # entra no grupo multicast onde as impressoras anunciam
        try:
            mreq = struct.pack("=4sl", socket.inet_aton("239.255.255.250"),
                               socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        except Exception:
            pass
        sock.settimeout(1.0)

        fim = time.time() + timeout
        while time.time() < fim:
            try:
                dados, origem = sock.recvfrom(4096)
            except socket.timeout:
                continue
            except Exception:
                break
            try:
                texto = dados.decode("utf-8", "ignore")
            except Exception:
                continue
            if "bambu" not in texto.lower() and "NOTIFY" not in texto.upper():
                continue
            campos = {}
            for linha in texto.splitlines():
                if ":" in linha:
                    chave, _, valor = linha.partition(":")
                    campos[chave.strip().lower()] = valor.strip()
            serial = campos.get("usn", "")
            if not serial:
                continue
            ip = origem[0]
            loc = campos.get("location", "")
            if loc and not loc.startswith("http"):
                ip = loc
            achadas[serial] = {
                "ip": ip,
                "serial": serial,
                "modelo": campos.get("devmodel.bambu.com", ""),
                "nome": campos.get("devname.bambu.com", ""),
            }
    except OSError as exc:
        return [], (f"Não consegui escutar a rede (porta 2021 em uso "
                    f"ou bloqueada pelo firewall): {exc}")
    except Exception as exc:
        return [], f"Erro na busca: {exc}"
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass
    return list(achadas.values()), None


@app.get("/api/bambu/buscar_lan")
async def api_bambu_buscar_lan(request: Request):
    """Procura impressoras Bambu que estejam se anunciando na rede local."""
    if (block := _need_auth(request)):
        return block
    achadas, erro = await asyncio.to_thread(_bambu_ssdp_scan, 6)
    return {"ok": erro is None, "impressoras": achadas, "error": erro}


def _testar_bambu_lan(ip, access_code, serial, timeout=10):
    """Tenta conectar numa Bambu em modo LAN e receber o primeiro relatório.
    Devolve (ok, mensagem, dados)."""
    resultado = {"conectou": False, "autorizado": False, "recebeu": False,
                 "modelo": None, "erro": None}
    pronto = threading.Event()

    def on_connect(client, userdata, flags, reason_code, properties=None):
        code = str(reason_code).lower()
        if reason_code == 0:
            resultado["conectou"] = True
            resultado["autorizado"] = True
            client.subscribe(f"device/{serial}/report")
            client.publish(f"device/{serial}/request",
                           json.dumps({"pushing": {"sequence_id": "0",
                                                   "command": "pushall"}}))
        else:
            resultado["conectou"] = True
            if "not authorized" in code or "unauthorized" in code:
                resultado["erro"] = "codigo_incorreto"
            else:
                resultado["erro"] = f"recusado: {reason_code}"
            pronto.set()

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
        except Exception:
            return
        resultado["recebeu"] = True
        info = data.get("print") or data.get("info") or {}
        modelo = info.get("printer_type") or data.get("printer_type")
        if modelo:
            resultado["modelo"] = modelo
        pronto.set()

    try:
        client = mqtt.Client(client_id=f"3dwork-teste-{int(time.time())}",
                             callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    except Exception:
        client = mqtt.Client(client_id=f"3dwork-teste-{int(time.time())}")
    client.username_pw_set("bblp", access_code)
    client.tls_set(cert_reqs=ssl.CERT_NONE)
    client.tls_insecure_set(True)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(ip, 8883, keepalive=20)
    except Exception as exc:
        return False, ("Não consegui alcançar a impressora nesse IP. "
                       "Confira se o número está certo e se ela está ligada "
                       "na mesma rede."), {"detalhe": str(exc)}

    client.loop_start()
    pronto.wait(timeout)
    client.loop_stop()
    try:
        client.disconnect()
    except Exception:
        pass

    if resultado["erro"] == "codigo_incorreto":
        return False, ("A impressora respondeu, mas recusou o Código de Acesso. "
                       "Confira o código na tela dela."), resultado
    if resultado["recebeu"]:
        return True, "Conectado!", resultado
    if resultado["autorizado"]:
        return False, ("Conectei, mas a impressora não respondeu. "
                       "Confira se o Número de Série está correto."), resultado
    return False, ("Não obtive resposta. Confira se o Modo LAN está ativado "
                   "na impressora."), resultado


@app.post("/api/bambu/testar_lan")
async def api_bambu_testar_lan(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    ip = (body.get("ip") or "").strip()
    code = (body.get("access_code") or "").strip()
    serial = (body.get("serial") or "").strip().upper()
    if not ip or not code or not serial:
        return {"ok": False, "error": "Preencha IP, Código de Acesso e Número de Série."}
    ok, msg, dados = await asyncio.to_thread(_testar_bambu_lan, ip, code, serial)
    return {"ok": ok, "mensagem": msg, "dados": dados}


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


@app.post("/api/printer/control")
async def api_printer_control(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    nome = body.get("name", "")
    acao = body.get("acao", "")
    ok, msg = await asyncio.to_thread(controlar_impressao, nome, acao)
    return {"ok": ok, "error": None if ok else msg,
            "aviso": msg if ok else None}


@app.get("/api/anycubic/raw")
async def api_anycubic_raw(request: Request):
    """Mostra os últimos dados brutos recebidos das impressoras Anycubic,
    para investigar campos (ex.: cor do filamento). Abra este endereço no
    navegador com a Kobra imprimindo e copie o resultado."""
    if (block := _need_auth(request)):
        return block
    with STATE_LOCK:
        saida = {}
        for nome, st in STATE.items():
            meta = st.get("_meta", {})
            if meta.get("brand") == "anycubic" or "anycubic" in str(meta.get("model", "")).lower():
                saida[nome] = st
        if not saida:
            # se não marcou marca, manda todas para o usuário ver
            saida = dict(STATE)
    return saida


@app.post("/api/anycubic/debug")
async def api_anycubic_debug(request: Request):
    if (block := _need_auth(request)):
        return block
    body = await request.json()
    ligar = bool(body.get("ligar"))
    _ANYCUBIC_DEBUG["on"] = ligar
    caminho = str(Path(__file__).with_name(_ANYCUBIC_DEBUG["arquivo"]))
    _ANYCUBIC_DEBUG["arquivo"] = caminho
    if ligar:
        try:
            open(caminho, "w", encoding="utf-8").close()
        except Exception:
            pass
    return {"ok": True, "ligado": ligar, "arquivo": caminho}


def _flashforge_scan(timeout=6):
    """Procura impressoras Flashforge na rede via broadcast UDP.
    A Flashforge responde a um broadcast na porta 19000/48899 com seus dados.
    Retorna lista de {ip, serial, nome, modelo}."""
    import socket
    achadas = {}
    # A Flashforge (5M/AD5X) responde a um broadcast de descoberta.
    # Tentamos as portas conhecidas de descoberta.
    portas_descoberta = [19000, 48899]
    mensagem = b"\xc0\xa8\x00\xde\x46\x50\x00\x00"  # padrão de descoberta FF

    for porta in portas_descoberta:
        sock = None
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(1.0)
            try:
                sock.sendto(mensagem, ("255.255.255.255", porta))
            except Exception:
                pass
            fim = time.time() + (timeout / len(portas_descoberta))
            while time.time() < fim:
                try:
                    dados, origem = sock.recvfrom(4096)
                except socket.timeout:
                    continue
                except Exception:
                    break
                ip = origem[0]
                if ip in achadas:
                    continue
                # tenta extrair nome/série da resposta (texto legível)
                try:
                    txt = dados.decode("utf-8", "ignore")
                except Exception:
                    txt = ""
                nome = ""
                for linha in txt.replace("\x00", "\n").split("\n"):
                    linha = linha.strip()
                    if linha and len(linha) > 2 and not linha.startswith("\\"):
                        nome = linha
                        break
                achadas[ip] = {"ip": ip, "serial": "", "nome": nome or "Flashforge",
                               "modelo": "AD5X"}
        except Exception:
            pass
        finally:
            if sock:
                try: sock.close()
                except Exception: pass

    # Para cada IP achado, tenta pegar o número de série pelo /detail
    # (sem check code, alguns firmwares respondem info básica; se não, fica vazio)
    for ip, info in achadas.items():
        try:
            d = _flashforge_detail(ip, "", "", timeout=3)
            if d:
                dd = d.get("detail", d)
                info["serial"] = dd.get("serialNumber", "") or info["serial"]
                info["modelo"] = dd.get("machineType", "") or info["modelo"]
        except Exception:
            pass

    return list(achadas.values())


@app.get("/api/flashforge/buscar")
async def api_flashforge_buscar(request: Request):
    if (block := _need_auth(request)):
        return block
    achadas = await asyncio.to_thread(_flashforge_scan, 6)
    return {"ok": True, "impressoras": achadas}


# ── Flashforge AD5X ───────────────────────────────────────────────
@app.post("/api/flashforge/testar")
async def api_flashforge_testar(request: Request):
    if (block := _need_auth(request)):
        return block
    b = await request.json()
    ip = (b.get("ip") or "").strip()
    serial = (b.get("serial") or "").strip()
    code = (b.get("check_code") or "").strip()
    if not ip or not code:
        return {"ok": False, "error": "Preencha o IP e o código de verificação."}
    detail = await asyncio.to_thread(_flashforge_detail, ip, serial, code)
    if detail is None:
        return {"ok": False, "error": ("Não consegui falar com a impressora. "
                "Confira o IP, se o Modo LAN está ativo e se o código está certo.")}
    d = detail.get("detail", detail)
    modelo = d.get("machineType") or d.get("printerName") or "Flashforge"
    return {"ok": True, "info": f"Modelo: {modelo}."}


@app.post("/api/flashforge/add")
async def api_flashforge_add(request: Request):
    if (block := _need_auth(request)):
        return block
    b = await request.json()
    ip = (b.get("ip") or "").strip()
    serial = (b.get("serial") or "").strip()
    code = (b.get("check_code") or "").strip()
    nome = (b.get("nome") or "").strip() or f"Flashforge {ip}"
    if not ip or not code:
        return {"ok": False, "error": "Dados incompletos."}
    # a Flashforge nem sempre expõe série; usa o IP como identificador se faltar
    cfg = {"name": nome, "brand": "flashforge", "mode": "lan",
           "ip": ip, "serial": serial or ip, "check_code": code,
           "model": "AD5X", "apelido": b.get("nome", "")}
    # add_printer_cfg já inicia a impressora internamente
    ok, err = await asyncio.to_thread(add_printer_cfg, cfg)
    if not ok:
        return {"ok": False, "error": err or "Já existe uma impressora com esse nome."}
    return {"ok": True}


# ── Estoque de filamento ──────────────────────────────────────────
@app.get("/api/estoque")
async def api_estoque_listar(request: Request):
    if (block := _need_auth(request)):
        return block
    return {"ok": True, "itens": estoque_listar()}


@app.post("/api/estoque/add")
async def api_estoque_add(request: Request):
    if (block := _need_auth(request)):
        return block
    b = await request.json()
    ok, err = await asyncio.to_thread(estoque_add_item,
        b.get("marca"), b.get("tipo"), b.get("cor"), b.get("cor_hex"),
        b.get("kg"), b.get("preco_kg"))
    return {"ok": ok, "error": err}


@app.post("/api/estoque/editar")
async def api_estoque_editar(request: Request):
    if (block := _need_auth(request)):
        return block
    b = await request.json()
    ok, err = await asyncio.to_thread(estoque_editar,
        b.get("id"), b.get("marca"), b.get("tipo"), b.get("cor"),
        b.get("cor_hex"), b.get("saldo_kg"), b.get("preco_kg"))
    return {"ok": ok, "error": err}


@app.post("/api/estoque/remover")
async def api_estoque_remover(request: Request):
    if (block := _need_auth(request)):
        return block
    b = await request.json()
    ok, err = await asyncio.to_thread(estoque_remover, b.get("id"))
    return {"ok": ok, "error": err}


@app.post("/api/estoque/descontar")
async def api_estoque_descontar(request: Request):
    if (block := _need_auth(request)):
        return block
    b = await request.json()
    ok, err, info = await asyncio.to_thread(estoque_descontar,
        b.get("id"), b.get("gramas"), b.get("printer"), b.get("obs"))
    return {"ok": ok, "error": err, "info": info}


@app.get("/api/estoque/relatorio")
async def api_estoque_relatorio(request: Request, inicio: str = "", fim: str = "",
                                 printers: str = "", item_id: str = ""):
    if (block := _need_auth(request)):
        return block
    plist = [p for p in printers.split(",") if p] if printers else None
    iid = int(item_id) if item_id.isdigit() else None
    rep = await asyncio.to_thread(estoque_relatorio,
        inicio or None, fim or None, plist, iid)
    return {"ok": True, **rep}


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


LOGO_DATA_URI = "data:image/png;base64,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133gx48dqF/WXXb9Y83Gkn/d6S9650619J3bGp3v3KIvfPuj/pq/HMouv3wwXPOW/o5TL9/Ve+pbt5ldbx0z112+xVz69v70oveOF1d/YPNY5p+qK8+5bh2eYh6onvnKO+ftX9mxt77gHx5sM9+9Wet527DZ+45t5oq3D9sL37Yv6rhiIOy8fG92weUD2UWXj3cu/8vdI51vqy1e/Vd7GoX39KdWva9hrf7kur6bqnPVZ907fvCrYb3zr3bVu9+xeaT7HTtk7V9uGV53+R1Lut+qn3bWW1KZN70lY+95ayaz7y25M571ltyZz7li89oV7xhctuGvhjpPeP9gvvffz3rnt+d8ibB00/WNpt39tQFt0btGeo/7ix3D6bdsTi9+1y7/pLffu2jZ28yTnvM287Tfe4d2xkXv1DIb3nnPqrXveiS17N07Fi19X3Xh6X891Hb83y0/ZtEn117xsfKBbLJ69YaSOVB73ZolJ7/k1BPOfN1JJ57+hyedfPKrTj7t1Fedvv70P1p/xvo/PvOss1511tlnbzr7nHNe+eyNG1+x4YwzXv7ci573srPPPfdlz3rOxped/6wLX3be+ee9lPGzzzv35Wede84rYpxz1h+efc7Zf3jWOWe/8qyzz5oE084+75xXnnPuuX943g3nvfLcC85/xfk3/eQPz7/xxlf+5Cc/edVNN930Kjxh2ETK+Pnnn/+qc88999VwiF9z5plnvg70daCXAH93yimnfBTxbz7rWc966EDtfCbllUZH14ZRVIQzLdgfxbZtwf4Yf/GTex4BR1O4BxLc90iZRiB8F54wjMxmk+XLl29bs2bN5SeffPKb4Li+7tRTT/3j00477bWgrz/l9FNf3z808IZHHn3kDec/64LXsG/Pu/D8P7zgWc96xTnnUv5AAQAAEABJREFUoy/PPedVURS8+dRTT/rhbPLnko4DkXfcccd9ZfHixS895phj3rRv374/Wrt27R9hHLwB9ngj2v7GRx999BLqikPWJWgDx8klOGBdirQ/AS5D2/5u9erVB1w/D0VHpVSAOoeg4w9R33/iQPa2kZGRN8L+m+B3vIKA8/0K4OXos5dB35fux0tAX4I6geBljUb9Zc1m/eVTMTY28krENzlOfdP4+OirisXCHxWL+T8GXlso5F8NyvCr4Nv8EeS8AT7QJThkvHHVqlU/R3xe39q8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"

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


def get_logo_bytes():
    """Retorna (bytes_da_imagem, formato) do logo atual — o do cliente se ele
    trocou, senão o padrão 3DWORK. Usado nos relatórios em PDF."""
    for nome in _LOGO_CUSTOM_NAMES:
        p = Path(__file__).with_name(nome)
        if p.exists():
            try:
                data = p.read_bytes()
                if len(data) > 0:
                    fmt = "JPEG" if nome.endswith(("jpg", "jpeg")) else "PNG"
                    return data, fmt
            except Exception:
                pass
    # padrão embutido (data URI -> bytes)
    try:
        cabec, b64 = LOGO_DATA_URI.split(",", 1)
        fmt = "JPEG" if "jpeg" in cabec else "PNG"
        return base64.b64decode(b64), fmt
    except Exception:
        return None, None


def get_logo_custom_bytes():
    """Retorna (bytes, formato) APENAS se o cliente trocou o logo pelo dele.
    Se ainda estiver usando o logo padrão 3DWORK, retorna (None, None).
    Usado no PDF de orçamento, que vai para o cliente final."""
    for nome in _LOGO_CUSTOM_NAMES:
        p = Path(__file__).with_name(nome)
        if p.exists():
            try:
                data = p.read_bytes()
                if len(data) > 0:
                    fmt = "JPEG" if nome.endswith(("jpg", "jpeg")) else "PNG"
                    return data, fmt
            except Exception:
                pass
    return None, None


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FarmSync · Farm de Impressoras</title>
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
  /* remove as setinhas (spinners) dos campos numéricos — atrapalhavam ao digitar */
  input[type=number]{-moz-appearance:textfield; appearance:textfield}
  input[type=number]::-webkit-inner-spin-button,
  input[type=number]::-webkit-outer-spin-button{-webkit-appearance:none; margin:0}
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
  .tb-brand{font-family:'JetBrains Mono',monospace; font-size:.74rem; letter-spacing:.06em;
    color:var(--chrome); font-weight:600; white-space:nowrap; opacity:.85}
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

  /* ── Estoque ──────────────────────────────────────────── */
  .mono{font-family:'JetBrains Mono',monospace}
  .rel-est-sec{margin-top:1.5rem}
  .rel-filtros{display:flex; gap:1.5rem; flex-wrap:wrap; align-items:flex-start}
  .rel-fg{display:flex; flex-direction:column; gap:.5rem}
  .rel-lbl{font-size:.8rem; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); font-weight:600}
  .rel-chips{display:flex; gap:.4rem; flex-wrap:wrap}
  .rel-chip{background:#0a0e16; border:1px solid var(--hair); border-radius:9px;
    padding:.5rem .9rem; cursor:pointer; color:var(--muted); font-family:inherit; font-size:.9rem; font-weight:500}
  .rel-chip:hover{border-color:var(--live); color:var(--ink)}
  .rel-chip.on{background:rgba(79,140,255,.14); border-color:var(--live); color:var(--live); font-weight:600}
  .rel-imps{display:flex; gap:.8rem; flex-wrap:wrap}
  .rel-imp{display:flex; align-items:center; gap:.4rem; font-size:.9rem; color:var(--muted); cursor:pointer}
  .rel-imp input{cursor:pointer}
  .rel-resumo{display:flex; gap:1rem; flex-wrap:wrap}
  .rel-rc{flex:1; min-width:150px; background:linear-gradient(160deg,#111826,#0c1119);
    border:1px solid var(--hair); border-radius:14px; padding:1rem 1.2rem; display:flex; flex-direction:column}
  .rel-rc small{font-size:.74rem; text-transform:uppercase; letter-spacing:.06em; color:var(--faint)}
  .rel-rc b{font-size:1.7rem; font-family:'JetBrains Mono',monospace; margin:.15rem 0; color:var(--done)}
  .rel-rc span{font-size:.82rem; color:var(--muted)}
  .dash-estoque-alerta{display:flex; align-items:center; gap:1rem; padding:1rem 1.2rem;
    background:rgba(255,140,50,.1); border:1px solid rgba(255,140,50,.35);
    border-radius:14px; margin-bottom:1.3rem}
  .dea-ic{font-size:1.6rem}
  .dea-txt{flex:1; display:flex; flex-direction:column; gap:.2rem}
  .dea-txt b{font-size:.98rem}
  .dea-txt span{font-size:.86rem; color:var(--muted)}
  .dea-btn{background:rgba(255,140,50,.18); color:#ff8c32; border:1px solid rgba(255,140,50,.4);
    border-radius:9px; padding:.5rem 1rem; cursor:pointer; font-family:inherit; font-weight:600; font-size:.88rem}
  .dea-btn:hover{background:rgba(255,140,50,.28)}
  .cf-desc{display:flex; align-items:center; gap:.5rem; font-size:.9rem; color:var(--muted);
    margin:.5rem 0 .3rem; cursor:pointer}
  .cf-desc input{width:auto; cursor:pointer}
  .est-top{display:flex; gap:1.1rem; flex-wrap:wrap; margin-bottom:1.4rem}
  .est-alertas{display:flex; flex-direction:column; gap:.6rem; margin-bottom:1.4rem}
  .est-aviso{padding:.9rem 1.1rem; border-radius:12px; font-size:.98rem;
    border:1px solid var(--hair); background:var(--bg2)}
  .est-aviso.est-atencao{border-color:rgba(224,169,79,.4); background:rgba(224,169,79,.08)}
  .est-aviso.est-baixo{border-color:rgba(255,140,50,.45); background:rgba(255,140,50,.1)}
  .est-aviso.est-critico,.est-aviso.est-negativo{border-color:rgba(255,84,112,.5); background:rgba(255,84,112,.12)}
  .est-sw{display:inline-block; width:14px; height:14px; border-radius:4px;
    border:1px solid #ffffff2e; margin-right:.5rem; vertical-align:middle}
  .est-form{display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:1rem}
  .est-form label{display:flex; flex-direction:column; gap:.4rem; font-size:.86rem; color:var(--muted)}
  .est-form input{background:#0a0e16; border:1px solid var(--hair); border-radius:9px;
    padding:.65rem .8rem; color:var(--ink); font-family:inherit; font-size:.98rem}
  .est-form input[type=color]{padding:.3rem; height:44px; cursor:pointer}
  .est-form input:focus{outline:none; border-color:var(--live); box-shadow:0 0 0 3px rgba(79,140,255,.12)}
  .est-hint{font-size:.84rem; color:var(--faint); margin-top:.7rem}
  .est-bd{display:inline-block; font-size:.74rem; font-weight:600; padding:.24rem .6rem; border-radius:20px}
  .est-bd-ok{background:rgba(55,209,124,.16); color:var(--done)}
  .est-bd-at{background:rgba(224,169,79,.16); color:var(--warn)}
  .est-bd-baixo{background:rgba(255,140,50,.18); color:#ff8c32}
  .est-bd-crit,.est-bd-neg{background:rgba(255,84,112,.18); color:var(--fail)}
  tr.est-critico td,tr.est-negativo td{background:rgba(255,84,112,.06)}
  tr.est-baixo td{background:rgba(255,140,50,.05)}
  /* ── Orçamentos ───────────────────────────────────────── */
  .q-top{display:flex; align-items:center; gap:1.1rem; flex-wrap:wrap; margin-bottom:1.5rem}
  .q-mini{background:linear-gradient(160deg,#111826,#0c1119); border:1px solid var(--hair);
    border-radius:14px; padding:1rem 1.3rem; display:flex; flex-direction:column; min-width:190px}
  .q-mini small{font-size:.74rem; text-transform:uppercase; letter-spacing:.07em; color:var(--faint)}
  .q-mini b{font-size:1.6rem; font-family:'JetBrains Mono',monospace; margin:.15rem 0}
  .q-mini span{font-size:.8rem; color:var(--muted)}
  .q-tablewrap{overflow-x:auto; border-radius:12px}
  .q-table{width:100%; border-collapse:collapse; font-size:.95rem}
  .q-table th{text-align:left; font-size:.76rem; text-transform:uppercase;
    letter-spacing:.05em; color:var(--muted); font-weight:600;
    padding:.7rem .7rem; border-bottom:1px solid var(--hair); white-space:nowrap}
  .q-table td{padding:.7rem .7rem; border-bottom:1px solid #131b28; vertical-align:middle}
  .q-table tbody tr:hover{background:#0f1622}
  .q-bd{display:inline-block; font-size:.76rem; font-weight:600; padding:.28rem .7rem;
    border-radius:20px; white-space:nowrap}
  .q-rasc{background:rgba(138,150,168,.16); color:#8a96a8}
  .q-env{background:rgba(79,140,255,.16); color:#4f8cff}
  .q-apr{background:rgba(53,209,124,.16); color:#37d67a}
  .q-rec{background:rgba(255,84,112,.16); color:#ff5470}
  .q-acts{display:flex; gap:.35rem; justify-content:flex-end}
  .q-acts button,.q-acts a{background:#0a0e16; border:1px solid var(--hair);
    border-radius:8px; width:34px; height:34px; display:flex; align-items:center;
    justify-content:center; cursor:pointer; color:var(--muted); font-size:.95rem;
    text-decoration:none; transition:all .15s}
  .q-acts button:hover,.q-acts a:hover{color:var(--live); border-color:var(--live)}
  .q-edithead{display:flex; align-items:center; gap:.8rem; flex-wrap:wrap; margin-bottom:1.5rem}
  .q-num{font-size:1.35rem; font-weight:700; font-family:'JetBrains Mono',monospace}
  .q-status{background:#0c1119; border:1px solid var(--hair); border-radius:10px;
    padding:.6rem .9rem; color:var(--ink); font-family:inherit; font-size:.95rem; cursor:pointer}
  .q-box{background:#0c1119; border:1px solid var(--hair); border-radius:16px;
    padding:1.5rem 1.6rem; margin-bottom:1.4rem}
  .q-box h3{margin:0 0 1.1rem; font-size:.86rem; text-transform:uppercase;
    letter-spacing:.06em; color:var(--muted); font-weight:600}
  .q-cli{display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:1.1rem}
  .q-cli label,.q-cond label,.q-obs{display:flex; flex-direction:column; gap:.4rem;
    font-size:.86rem; color:var(--muted)}
  .q-cli input,.q-cond input,.q-obs textarea,.q-table input,.q-table select{
    background:#0a0e16; border:1px solid var(--hair); border-radius:9px;
    padding:.65rem .8rem; color:var(--ink); font-family:inherit; font-size:.98rem; width:100%}
  .q-cli input:focus,.q-cond input:focus,.q-obs textarea:focus,
  .q-table input:focus,.q-table select:focus{outline:none; border-color:var(--live);
    box-shadow:0 0 0 3px rgba(79,140,255,.12)}
  .q-obs textarea{resize:vertical; font-size:.95rem; min-height:70px}
  .q-cond{display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
    gap:1rem; margin-bottom:1.1rem}
  .q-itens input,.q-itens select{padding:.55rem .6rem; font-size:.95rem}
  .qnum{text-align:right; font-family:'JetBrains Mono',monospace}
  .q-itens{min-width:900px}
  /* cada campo com largura confortável */
  .q-itens input[data-f=peso_g]{width:64px}
  .q-itens select[data-f=peso_unid]{width:54px; padding:.55rem .3rem}
  .q-itens input[data-f=tempo_min]{width:72px}
  .q-itens input[data-f=valor_filamento]{width:92px}
  .q-itens input[data-f=qtd]{width:60px}
  .q-itens input[data-f=valor_venda]{width:96px}
  .q-tempo{display:flex; gap:.3rem; align-items:center}
  .q-tempo .q-tempo-u{width:52px; flex:0 0 52px; padding:.55rem .3rem}
  .q-databox{padding:1.1rem 1.3rem}
  .q-datalbl{display:flex; align-items:center; gap:.7rem; font-size:1rem; color:var(--ink); font-weight:600}
  .q-datalbl input{background:#0a0e16; border:1px solid var(--hair); border-radius:9px;
    padding:.6rem .8rem; color:var(--ink); font-size:1rem; font-family:inherit}
  .q-sub{text-align:right; font-family:'JetBrains Mono',monospace; font-weight:700;
    color:var(--done); white-space:nowrap; font-size:1rem}
  .q-x{background:transparent; border:0; color:var(--faint); cursor:pointer;
    font-size:1.1rem; padding:.3rem .5rem; border-radius:7px}
  .q-x:hover{color:var(--fail); background:rgba(255,84,112,.1)}
  .q-totbox{display:flex; flex-direction:column}
  .q-totais{display:flex; flex-direction:column; gap:.6rem}
  .qt-l{display:flex; justify-content:space-between; align-items:center; font-size:1rem}
  .qt-l span{color:var(--muted)}
  .qt-l b{font-family:'JetBrains Mono',monospace}
  .qt-desc b{color:var(--warn)}
  .qt-total{display:flex; justify-content:space-between; align-items:baseline;
    border-top:1px solid var(--hair); padding-top:.9rem; margin-top:.4rem}
  .qt-total span{font-size:.86rem; letter-spacing:.06em; color:var(--faint)}
  .qt-total b{font-size:2.1rem; color:var(--done); font-family:'JetBrains Mono',monospace}
  .qt-margem{margin-top:1.1rem; padding:1rem 1.1rem; background:rgba(120,140,170,.08);
    border:1px dashed var(--hair); border-radius:12px}
  .qt-mrow{display:flex; justify-content:space-between; align-items:baseline; font-size:.95rem;
    color:var(--muted); margin-bottom:.4rem}
  .qt-mrow b{font-family:'JetBrains Mono',monospace; color:var(--ink)}
  .qt-lucro b{color:var(--done)}
  .qt-mhint{font-size:.82rem; color:var(--faint); margin-top:.5rem; line-height:1.5}
  .qt-info{font-size:.76rem; color:var(--muted); margin-top:.3rem}
  .qt-hint{font-size:.72rem; color:var(--faint); line-height:1.5}
  .qt-hint a{color:var(--live)}

  /* ── Mural de atualizações ────────────────────────────── */
  .mural-wrap{max-width:760px}
  .mural-intro{display:flex; align-items:center; gap:1rem; background:#0c1119;
    border:1px solid var(--hair); border-radius:14px; padding:1.1rem 1.3rem; margin-bottom:1.6rem}
  .mural-intro-ic{font-size:1.8rem}
  .mural-intro-t{font-size:1.05rem; font-weight:700}
  .mural-intro-s{font-size:.85rem; color:var(--muted); margin-top:.2rem}
  .mural-timeline{position:relative}
  .mural-item{position:relative; padding-left:2.2rem; padding-bottom:1.4rem}
  .mural-line{position:absolute; left:9px; top:0; bottom:0; width:2px; background:var(--hair)}
  .mural-item:last-child .mural-line{bottom:auto; height:22px}
  .mural-dot{position:absolute; left:0; top:4px; width:20px; height:20px; border-radius:50%;
    background:#0c1119; border:2px solid var(--hair); display:flex; align-items:center;
    justify-content:center; font-size:.62rem; color:var(--faint); z-index:1}
  .mural-nova .mural-dot{border-color:var(--live); color:var(--live);
    box-shadow:0 0 0 4px rgba(79,140,255,.14)}
  .mural-card{background:#0c1119; border:1px solid var(--hair); border-radius:12px;
    padding:.9rem 1.1rem}
  .mural-nova .mural-card{border-color:rgba(79,140,255,.4)}
  .mural-head{display:flex; align-items:center; gap:.6rem; flex-wrap:wrap; margin-bottom:.3rem}
  .mural-ver{font-family:'JetBrains Mono',monospace; font-weight:700; font-size:.92rem}
  .mural-badge{font-size:.66rem; font-weight:600; text-transform:uppercase; letter-spacing:.05em;
    background:rgba(79,140,255,.16); color:var(--live); padding:.15rem .5rem; border-radius:20px}
  .mural-data{margin-left:auto; font-size:.75rem; color:var(--faint)}
  .mural-titulo{font-size:.95rem; font-weight:600; color:var(--chrome); margin-bottom:.5rem}
  .mural-lista{margin:0; padding-left:1.1rem; display:flex; flex-direction:column; gap:.35rem}
  .mural-lista li{font-size:.86rem; color:var(--muted); line-height:1.5}

  /* ── Acesso remoto ────────────────────────────────────── */
  .rem-off,.rem-on{text-align:center; padding:.5rem}
  .rem-ic{font-size:2.6rem; margin-bottom:.6rem}
  .rem-txt{font-size:.9rem; color:var(--ink); line-height:1.6; margin:.4rem 0}
  .rem-hint{font-size:.78rem; color:var(--muted); line-height:1.5; margin:.4rem 0 0}
  .rem-badge{display:inline-block; background:rgba(55,211,153,.16); color:var(--done);
    font-weight:700; font-size:.9rem; padding:.4rem .9rem; border-radius:20px; margin-bottom:.6rem}
  .rem-url{display:flex; gap:.4rem; margin:.9rem 0}
  .rem-url input{flex:1; background:#0a0e16; border:1px solid var(--hair); border-radius:8px;
    padding:.55rem .7rem; color:var(--live); font-family:'JetBrains Mono',monospace; font-size:.8rem}
  .rem-url button{background:var(--live); border:0; border-radius:8px; color:#fff;
    padding:.55rem 1rem; cursor:pointer; font-family:inherit; font-size:.82rem; white-space:nowrap}
  .rem-qr{width:180px; height:180px; border-radius:12px; margin:.6rem auto 0; display:block;
    background:#fff; padding:8px}
  .rem-erro{color:var(--warn); font-size:.9rem; margin-bottom:1rem}
  .rem-load{text-align:center; padding:2rem 1rem}
  .rem-load p{font-size:.88rem; color:var(--muted); margin-top:1rem; line-height:1.5}
  .rem-spin{width:38px; height:38px; border:3px solid var(--hair); border-top-color:var(--live);
    border-radius:50%; margin:0 auto; animation:remspin .8s linear infinite}
  @keyframes remspin{to{transform:rotate(360deg)}}
  .rem-cfg{padding:.3rem}
  .rem-cfg label{display:block; font-size:.82rem; color:var(--muted); margin:.8rem 0 .3rem}
  .rem-cfg input[type=text],.rem-cfg input[type=password],.rem-cfg input:not([type]){
    width:100%; background:#0a0e16; border:1px solid var(--hair); border-radius:8px;
    padding:.6rem .7rem; color:var(--ink); font-size:.86rem; box-sizing:border-box}
  .rem-check{display:flex !important; align-items:center; gap:.5rem; font-size:.84rem !important;
    color:var(--ink) !important; margin-top:1rem !important; cursor:pointer}
  .rem-check input{width:auto !important}

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
  .senha-banner{background:linear-gradient(90deg,rgba(255,204,68,.16),rgba(255,122,61,.14));
    border-bottom-color:rgba(255,204,68,.4)}
  .senha-banner b{color:#ffcc44}

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
    position:relative; display:flex; gap:.7rem; padding:.85rem .95rem .65rem;
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

  .row1{display:flex; gap:.85rem}

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

  .body{flex:1; min-width:0; display:flex; flex-direction:column; gap:.5rem}
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
    margin-top:.35rem; border:1px solid var(--hair)}
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

  .foot{display:flex; justify-content:center}
  .foot:not(:empty){min-height:.9rem; margin-top:.35rem}
  .ctrl-wrap:not(:empty){margin-top:.5rem}
  .ctrl-btns{display:flex; gap:.4rem; justify-content:flex-end}
  .ctrl-btn{padding:.28rem .6rem; border-radius:7px; border:1px solid var(--hair);
    background:transparent; color:var(--faint); cursor:pointer; font-family:inherit;
    font-size:.72rem; font-weight:500; opacity:.7; transition:all .15s}
  .ctrl-btn:hover{opacity:1; background:#0f1622}
  .ctrl-pause:hover{border-color:rgba(255,176,32,.4); color:#ffb020}
  .ctrl-resume:hover{border-color:rgba(55,211,153,.4); color:var(--done)}
  .ctrl-stop:hover{border-color:rgba(255,90,90,.4); color:#ff5a5a}
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
  body.kiosk main{gap:1.6rem; padding:1.6rem; grid-template-columns:repeat(auto-fit,minmax(520px,1fr))}
  body.kiosk .card{padding:1.7rem 1.7rem 1.2rem}
  body.kiosk .chamber{width:148px; flex:0 0 148px; height:264px}
  body.kiosk .chamber .pc{font-size:2.5rem}
  body.kiosk .chamber .ly{font-size:.82rem}
  body.kiosk .pname{font-size:2.1rem}
  body.kiosk .pill{font-size:.86rem; padding:.42rem .85rem}
  /* Modo parede mostra TODAS as informações, maiores e legíveis de longe */
  body.kiosk .tags{gap:.5rem; margin-top:.5rem}
  body.kiosk .tag{font-size:.82rem; padding:.32rem .7rem}
  body.kiosk .pbar{height:14px; margin-top:.7rem}
  body.kiosk .env{font-size:1.05rem; gap:1.2rem; margin-top:.6rem}
  body.kiosk .env b{font-size:1.15rem}
  body.kiosk .ams-wrap{margin-top:.7rem}
  body.kiosk .ams-slot{transform:scale(1.15); transform-origin:left center}
  body.kiosk .obj{font-size:1.45rem; white-space:normal}
  body.kiosk .stage{font-size:.86rem}
  /* mostra as 4 métricas (antes escondia da 3ª em diante) */
  body.kiosk .metrics{grid-template-columns:repeat(2,1fr); gap:.8rem 1.4rem; margin-top:.6rem}
  body.kiosk .metrics .k{font-size:.78rem}
  body.kiosk .metrics .v{font-size:1.9rem}
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
  .bl-res{margin-top:.9rem; padding:.7rem .9rem; border-radius:10px; font-size:.86rem; line-height:1.5}
  .bl-res small{color:var(--muted); font-size:.78rem}
  .bl-testando{background:rgba(79,140,255,.12); border:1px solid rgba(79,140,255,.35); color:var(--live)}
  .bl-ok{background:rgba(55,211,153,.12); border:1px solid rgba(55,211,153,.4); color:var(--done)}
  .bl-erro{background:rgba(255,84,112,.12); border:1px solid rgba(255,84,112,.4); color:var(--fail)}
  .bl-lista{display:flex; flex-direction:column; gap:.4rem; margin-top:.7rem}
  .bl-item{background:#0a0e16; border:1px solid var(--hair); border-radius:10px;
    padding:.6rem .8rem; cursor:pointer; transition:border-color .15s, background .15s}
  .bl-item:hover{border-color:var(--live); background:#0d1421}
  .bl-i-nome{font-weight:600; font-size:.9rem}
  .bl-i-det{font-size:.76rem; color:var(--muted); font-family:'JetBrains Mono',monospace}
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
    .tb-brand{display:none}

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
      <img class="sb-logo" src="__LOGO_SRC__" alt="FarmSync">
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
      <button class="sb-item" data-page="quotes" onclick="nav('quotes')">
        <span class="sb-ic">📝</span><span class="sb-tx">Orçamentos</span></button>
      <button class="sb-item" data-page="estoque" onclick="nav('estoque')">
        <span class="sb-ic">📦</span><span class="sb-tx">Estoque</span></button>
      <button class="sb-item" data-page="calc" onclick="nav('calc')">
        <span class="sb-ic">🧮</span><span class="sb-tx">Calculadora</span></button>
      <button class="sb-item" data-page="settings" onclick="nav('settings')">
        <span class="sb-ic">⚙</span><span class="sb-tx">Configurações</span></button>
      <button class="sb-item" data-page="mural" onclick="nav('mural')">
        <span class="sb-ic">📣</span><span class="sb-tx">Mural de atualizações</span></button>
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
      <div class="tb-brand">FARMSYNC — FARM DE IMPRESSORAS 3D</div>
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

    <!-- PÁGINA: ORÇAMENTOS -->
    <div class="page" id="page-quotes" style="display:none">
      <div id="quotesContent"></div>
    </div>

    <!-- PÁGINA: PROJETOS -->
    <div class="page" id="page-projects" style="display:none">
      <div id="projectsContent"></div>
    </div>

    <!-- PÁGINA: ESTOQUE -->
    <div class="page" id="page-estoque" style="display:none">
      <div id="estoqueContent"></div>
    </div>

    <!-- PÁGINA: CALCULADORA -->
    <div class="page" id="page-calc" style="display:none">
      <div id="calcContent"></div>
    </div>

    <!-- PÁGINA: CONFIGURAÇÕES -->
    <div class="page" id="page-settings" style="display:none">
      <div id="settingsContent"></div>
    </div>

    <!-- PÁGINA: MURAL DE ATUALIZAÇÕES -->
    <div class="page" id="page-mural" style="display:none">
      <div id="muralContent"></div>
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
<div class="overlay" id="genOverlay" onclick="if(event.target===this)fecharModalGenerico()">
  <div class="modal" id="genModal" style="width:min(480px,96vw)"></div>
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
// Códigos de erro "fantasma" da Bambu (reportados com a impressora normal).
const ERROS_IGNORADOS = new Set([0x500C011]);  // 83935249 — não documentado pela Bambu
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
  if(p.print_error&&p.print_error!==0&&!_erroIgnorado(p.print_error)) cur.add("Erro 0x"+(p.print_error>>>0).toString(16).toUpperCase());
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
  let temAms=false;
  if(Array.isArray(units)){
    units.forEach((u,ui)=>(u.tray||[]).forEach((t,ti)=>{
      const gi=ui*4+parseInt(t.id??ti), empty=!t.tray_type;
      const col=empty?"transparent":trayColor(t.tray_color);
      const ty=empty?"vazio":t.tray_type;
      const rm=(t.remain!=null&&t.remain>=0)?`<span class="rm">${t.remain}%</span>`:"";
      slots.push(`<div class="slot${gi===now?" active":""}"><span class="sw" style="background:${col}"></span>${ty}${rm}</div>`);
      temAms=true;
    }));
  }
  // Bambu sem AMS: usa a bobina externa (vt_tray). Mostra mesmo que só tenha a cor.
  if(!temAms && p.vt_tray){
    const t=p.vt_tray;
    const temCor = t.tray_color && t.tray_color!=="00000000" && String(t.tray_color).length>=6;
    if(t.tray_type || temCor){
      const ty=t.tray_type||"Filamento";
      const col=temCor?trayColor(t.tray_color):"transparent";
      slots.push(`<div class="slot"><span class="sw" style="background:${col}"></span>${ty}</div>`);
    }
  }
  return slots.length?`<div class="ams"><span class="lbl">Filamento</span>${slots.join("")}</div>`:"";
}
// Códigos de erro "fantasma" da Bambu — reportados mesmo com a impressora
// funcionando normal, e não documentados pela própria Bambu. Ignorados para
// não poluir a tela. Outros erros continuam aparecendo normalmente.
function _erroIgnorado(code){ return ERROS_IGNORADOS.has(code>>>0); }

function alertHtml(p){
  const items=[];
  if(p.print_error&&p.print_error!==0&&!_erroIgnorado(p.print_error))
    items.push("Erro 0x"+(p.print_error>>>0).toString(16).toUpperCase());
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
    <div class="ctrl-wrap"></div>
    <div class="foot"></div>`;
  const q=s=>root.querySelector(s);
  const refs={root, fill:q(".fill"), scan:q(".scan"), pc:q(".pc"), ly:q(".ly"),
    pname:q(".pname"), tags:q(".tags"), pill:q(".pill"), stage:q(".stage"),
    obj:q(".obj"), pbar:q(".pbar i"), env:q(".env"), ams:q(".ams-wrap"),
    alert:q(".alert-wrap"), ctrl:q(".ctrl-wrap"),
    m:[...root.querySelectorAll(".metrics .v")],
    _tags:"", _env:"", _ams:"", _alert:"", _ctrl:"", camOn:false};
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
  if(setHTML(r,"_ctrl",ctrlHtml(p,online,name))) r.ctrl.innerHTML=r._ctrl;
  recordHistory(name,p,online);
  verificarNotificacao(name,st,online);
}

// ── Notificações: avisa quando uma impressão TERMINA ou FALHA ──────────
const _estadoAnterior = {};   // nome -> último gcode_state visto
function verificarNotificacao(name,st,online){
  if(!online) return;
  const p=st.print||{};
  const meta=st._meta||{};
  const estado=p.gcode_state;
  const anterior=_estadoAnterior[name];
  _estadoAnterior[name]=estado;
  // só dispara na TRANSIÇÃO para FINISH/FAILED (não a cada atualização)
  if(anterior===undefined) return;   // primeira leitura, não avisa
  if(estado===anterior) return;
  const estavaImprimindo=["RUNNING","PREPARE","PAUSE","SLICING"].includes(anterior);
  if(!estavaImprimindo) return;
  const disp=(meta.apelido||"").trim()||name;
  if(estado==="FINISH"){
    dispararAviso("terminou", disp, "✅ Impressão concluída", `${disp} terminou de imprimir.`);
  }else if(estado==="FAILED"){
    dispararAviso("falhou", disp, "⚠️ Impressão com falha", `${disp} teve uma falha na impressão.`);
  }
}
function _notifCfg(){
  try{ return JSON.parse(localStorage.getItem("notifCfg")||"{}"); }catch(_){ return {}; }
}
function dispararAviso(tipo, impressora, titulo, corpo){
  const cfg=_notifCfg();
  const ligado = tipo==="terminou" ? cfg.terminou!==false : cfg.falhou!==false;
  if(!ligado) return;
  // 1) som
  if(cfg.som!==false) tocarSom(tipo);
  // 2) notificação do navegador (Windows)
  if(cfg.navegador!==false && "Notification" in window && Notification.permission==="granted"){
    try{ new Notification(titulo,{body:corpo, icon:"__LOGO_SRC__"}); }catch(_){}
  }
}
let _audioCtx=null;
function tocarSom(tipo){
  try{
    _audioCtx=_audioCtx||new (window.AudioContext||window.webkitAudioContext)();
    const ctx=_audioCtx;
    // terminou: dois tons subindo (alegre). falhou: dois tons descendo (alerta).
    const notas = tipo==="terminou" ? [660,880] : [440,330];
    notas.forEach((f,i)=>{
      const o=ctx.createOscillator(), g=ctx.createGain();
      o.frequency.value=f; o.type="sine";
      o.connect(g); g.connect(ctx.destination);
      const t=ctx.currentTime+i*0.18;
      g.gain.setValueAtTime(0,t);
      g.gain.linearRampToValueAtTime(0.25,t+0.02);
      g.gain.linearRampToValueAtTime(0,t+0.16);
      o.start(t); o.stop(t+0.18);
    });
  }catch(_){}
}

function ctrlHtml(p,online,name){
  if(!online) return "";
  const st=p.gcode_state;
  const nm=name.replace(/'/g,"\\'");
  // imprimindo -> pausar + cancelar; pausado -> retomar + cancelar
  if(st==="RUNNING"||st==="PREPARE"){
    return `<div class="ctrl-btns">
      <button class="ctrl-btn ctrl-pause" onclick="event.stopPropagation();controlarImpressao('${nm}','pause')">⏸ Pausar</button>
      <button class="ctrl-btn ctrl-stop" onclick="event.stopPropagation();controlarImpressao('${nm}','stop')">⏹ Cancelar</button>
    </div>`;
  }
  if(st==="PAUSE"){
    return `<div class="ctrl-btns">
      <button class="ctrl-btn ctrl-resume" onclick="event.stopPropagation();controlarImpressao('${nm}','resume')">▶ Retomar</button>
      <button class="ctrl-btn ctrl-stop" onclick="event.stopPropagation();controlarImpressao('${nm}','stop')">⏹ Cancelar</button>
    </div>`;
  }
  return "";
}
async function controlarImpressao(name,acao){
  if(acao==="stop"){
    if(!confirm(`Cancelar a impressão de "${name}"?\n\nIsso vai PARAR a impressão em andamento. Não dá para desfazer.`)) return;
  }
  try{
    const d=await (await fetch("/api/printer/control",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({name,acao})})).json();
    if(!d.ok){
      alert("Não foi possível: "+(d.error||"erro desconhecido"));
    }else if(d.aviso){
      alert(d.aviso);
    }
  }catch(_){
    alert("Erro de conexão ao enviar o comando.");
  }
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
  reports:"Relatórios",quotes:"Orçamentos",estoque:"Estoque",calc:"Calculadora",settings:"Configurações",mural:"Mural de atualizações"};
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
  // ao sair da dashboard, marca para reanimar na próxima entrada
  if(page!=="dashboard"){
    const db=document.getElementById("dashContent");
    if(db) db.dataset.montada="";
  }
  if(page==="dashboard") renderDash(true);
  else if(page==="printers") applyViewMode();
  else if(page==="reports") mountReports();
  else if(page==="quotes") renderQuotes();
  else if(page==="projects") renderProjects();
  else if(page==="calc") mountCalc();
  else if(page==="estoque") renderEstoque();
  else if(page==="settings") renderSettings();
  else if(page==="mural") renderMural();
}
function toggleSidebar(){
  document.querySelector(".app")?.classList.toggle("sb-open");
}

/* ── Dashboard (home) ─────────────────────────────────── */
let dashPeriod="mes";
function contarEstados(printers){
  let imprimindo=0, ociosas=0, erro=0, offline=0;
  for(const [n,st] of printers){
    const meta=st._meta||{}, p=st.print||{};
    if(!meta.online){ offline++; continue; }
    const info=STATES[p.gcode_state];
    const cls=info?info[0]:null;
    if(cls==="printing") imprimindo++;
    else if(cls==="failed") erro++;
    else ociosas++;
  }
  return {imprimindo, ociosas, erro, offline, total:printers.length};
}
function atualizarKpisAoVivo(printers){
  // recalcula e atualiza só os NÚMEROS (sem redesenhar a página)
  const c=contarEstados(printers);
  const box=document.getElementById("dashContent");
  if(!box) return;
  const elImp=box.querySelector(".k-live [data-count]");
  if(elImp){ elImp.setAttribute("data-count",c.imprimindo); elImp.textContent=c.imprimindo; }
  const elTot=box.querySelector(".k-live .kpi-v span:last-child");
  if(elTot) elTot.textContent="/"+c.total;
  const elSub=box.querySelector(".k-live .kpi-sub");
  if(elSub) elSub.textContent=`${c.ociosas} ociosas · ${c.erro} erro · ${c.offline} offline`;
}
async function renderDash(anima){
  // anima = true  -> monta a página e roda as animações (1 vez)
  // anima = false -> só atualiza os cards de impressora ao vivo (sem reanimar)
  const box=document.getElementById("dashContent");
  if(!box) return;
  const printers=Object.entries(lastData||{});

  // atualização ao vivo (chamada pelo WebSocket): não redesenha tudo,
  // só troca os cards de impressora — assim nada pisca nem reanima
  if(anima===false && box.dataset.montada==="1"){
    const grid=box.querySelector(".pcard-grid");
    if(grid){
      grid.innerHTML=renderPrinterCards(printers);
      animateProgRings();   // só os anéis de progresso, que refletem o estado atual
    }
    atualizarKpisAoVivo(printers);   // recalcula os números (imprimindo/ociosas/etc)
    return;
  }

  const _c=contarEstados(printers);
  let imprimindo=_c.imprimindo, ociosas=_c.ociosas, erro=_c.erro, offline=_c.offline;
  const totalP=printers.length;
  let rep=null;
  try{ rep=await (await fetch("/api/report?period="+dashPeriod)).json(); }catch(_){}

  // alertas de estoque de filamento
  let estAlertas=[];
  try{
    const est=(await (await fetch("/api/estoque")).json()).itens||[];
    estAlertas=est.filter(i=>i.alerta);
  }catch(_){}
  const bannerEstoque=estAlertas.length?`
    <div class="dash-estoque-alerta">
      <span class="dea-ic">📦</span>
      <div class="dea-txt">
        <b>Estoque de filamento em alerta</b>
        <span>${estAlertas.map(i=>`${escapeHtml(i.marca)} ${escapeHtml(i.tipo)} ${escapeHtml(i.cor)} (${i.saldo_kg.toFixed(1)}kg)`).join(" · ")}</span>
      </div>
      <button class="dea-btn" onclick="nav('estoque')">Ver estoque</button>
    </div>`:"";

  box.innerHTML=`
    <div class="dash-grid">
      ${bannerEstoque}
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
  box.dataset.montada="1";
  animateCounts();
  animateProgRings();
}
function setDashPeriod(p){ dashPeriod=p; renderDash(true); }
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
/* ── Mural de atualizações ────────────────────────────── */
async function renderMural(){
  const box=document.getElementById("muralContent");
  if(!box) return;
  box.innerHTML=`<div class="dash-empty">Carregando…</div>`;
  let itens=[], atual="";
  try{
    const d=await (await fetch("/api/changelog")).json();
    if(d.ok){ itens=d.itens||[]; atual=d.versao_atual||""; }
  }catch(_){}

  if(!itens.length){
    box.innerHTML=`<div class="dash-empty"><b>Nada por aqui ainda</b>As novidades das próximas atualizações aparecerão neste mural.</div>`;
    return;
  }

  const blocos=itens.map((it,i)=>{
    const eAtual = it.versao===atual;
    const novidades=(it.novidades||[]).map(n=>`<li>${escHtml(n)}</li>`).join("");
    return `
      <div class="mural-item ${i===0?'mural-nova':''}">
        <div class="mural-line"></div>
        <div class="mural-dot">${i===0?'★':''}</div>
        <div class="mural-card">
          <div class="mural-head">
            <span class="mural-ver">Versão ${escHtml(it.versao)}</span>
            ${eAtual?`<span class="mural-badge">você está usando esta</span>`:''}
            ${it.data?`<span class="mural-data">${escHtml(it.data)}</span>`:''}
          </div>
          ${it.titulo?`<div class="mural-titulo">${escHtml(it.titulo)}</div>`:''}
          <ul class="mural-lista">${novidades}</ul>
        </div>
      </div>`;
  }).join("");

  box.innerHTML=`
    <div class="mural-wrap">
      <div class="mural-intro">
        <div class="mural-intro-ic">📣</div>
        <div>
          <div class="mural-intro-t">O que há de novo no FarmSync</div>
          <div class="mural-intro-s">Acompanhe aqui tudo o que cada atualização trouxe de melhorias e novidades.</div>
        </div>
      </div>
      <div class="mural-timeline">${blocos}</div>
    </div>`;
}
function escHtml(s){ return String(s==null?"":s)
  .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;"); }

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
      <button class="set-card" onclick="abrirNotificacoes()">
        <span class="set-ic">🔔</span>
        <span class="set-tx"><b>Avisos de impressão</b><small>Som e notificação quando terminar ou falhar</small></span></button>
      <button class="set-card" onclick="abrirAcessoRemoto()">
        <span class="set-ic">🌐</span>
        <span class="set-tx"><b>Acesso remoto</b><small>Ver as impressoras de fora da sua rede</small></span></button>
      <button class="set-card" onclick="checkUpdate(false)">
        <span class="set-ic">🔄</span>
        <span class="set-tx"><b>Verificar atualização</b><small>Versão atual: __APP_VERSION__</small></span></button>
    </div>`;
}
function openCalcConfig(){ nav('calc'); }

/* ── Avisos de impressão (notificação + som) ───────────── */
function abrirNotificacoes(){
  const cfg=_notifCfg();
  const on=(k,padrao=true)=> (cfg[k]!==false)===padrao ? "checked" : (cfg[k]!==false?"checked":"");
  const chk=(k)=> cfg[k]!==false ? "checked" : "";
  const permNav = ("Notification" in window) ? Notification.permission : "indisponivel";
  let permAviso = "";
  if(permNav==="denied") permAviso=`<div class="rem-erro" style="margin-top:.5rem">As notificações estão bloqueadas no navegador. Libere nas configurações do navegador (cadeado ao lado do endereço) para receber avisos na tela.</div>`;
  const html=`
    <div class="wiz-head"><b>🔔 Avisos de impressão</b><div class="mclose" onclick="fecharModalGenerico()">✕</div></div>
    <div class="wiz-body">
      <p class="rem-txt">Escolha como quer ser avisado quando uma impressão terminar ou falhar.</p>
      <label class="rem-check"><input type="checkbox" id="ntTerminou" ${chk("terminou")}> Avisar quando <b>terminar</b></label>
      <label class="rem-check"><input type="checkbox" id="ntFalhou" ${chk("falhou")}> Avisar quando <b>falhar</b></label>
      <hr style="border:0;border-top:1px solid var(--hair);margin:1rem 0">
      <label class="rem-check"><input type="checkbox" id="ntSom" ${chk("som")}> Tocar um <b>som</b> de alerta</label>
      <label class="rem-check"><input type="checkbox" id="ntNav" ${chk("navegador")}> Mostrar <b>notificação</b> na tela (Windows)</label>
      ${permAviso}
      <div style="display:flex;gap:.5rem;margin-top:1rem">
        <button class="wiz-btn" onclick="testarAviso()">🔊 Testar aviso</button>
        <button class="wiz-btn primary" style="flex:1" onclick="salvarNotificacoes()">Salvar</button>
      </div>
    </div>`;
  mostrarModalGenerico(html);
}
async function salvarNotificacoes(){
  const cfg={
    terminou: document.getElementById("ntTerminou").checked,
    falhou: document.getElementById("ntFalhou").checked,
    som: document.getElementById("ntSom").checked,
    navegador: document.getElementById("ntNav").checked,
  };
  try{ localStorage.setItem("notifCfg", JSON.stringify(cfg)); }catch(_){}
  // se ligou a notificação do navegador, pede permissão
  if(cfg.navegador && "Notification" in window && Notification.permission==="default"){
    try{ await Notification.requestPermission(); }catch(_){}
  }
  fecharModalGenerico();
}
function testarAviso(){
  // salva o estado atual dos checkboxes temporariamente e dispara um teste
  const som=document.getElementById("ntSom").checked;
  const nav=document.getElementById("ntNav").checked;
  if(som) tocarSom("terminou");
  if(nav && "Notification" in window){
    if(Notification.permission==="granted"){
      try{ new Notification("✅ Teste de aviso",{body:"É assim que você será avisado."}); }catch(_){}
    }else if(Notification.permission==="default"){
      Notification.requestPermission().then(pp=>{
        if(pp==="granted") try{ new Notification("✅ Teste de aviso",{body:"É assim que você será avisado."}); }catch(_){}
      });
    }
  }
}

/* ── Acesso remoto (Cloudflare Tunnel) ────────────────── */
async function abrirAcessoRemoto(){
  let html=`<div class="wiz-head"><b>🌐 Acesso remoto</b><div class="mclose" onclick="fecharModalGenerico()">✕</div></div>
    <div class="wiz-body" id="remotoBody"><div class="dash-empty">Verificando…</div></div>`;
  mostrarModalGenerico(html);
  await atualizarRemoto();
}
async function atualizarRemoto(){
  const body=document.getElementById("remotoBody");
  if(!body) return;
  let st={status:"desligado"};
  try{ st=await (await fetch("/api/remoto/status")).json(); }catch(_){}

  if(st.status==="ligado" && st.url){
    const qr="https://api.qrserver.com/v1/create-qr-code/?size=180x180&data="+encodeURIComponent(st.url);
    body.innerHTML=`
      <div class="rem-on">
        <div class="rem-badge">✓ Acesso remoto ativo${st.fixo?" · endereço fixo":""}</div>
        <p class="rem-txt">Abra este endereço no celular ou em qualquer computador,
          de qualquer lugar. Vai pedir seu usuário e senha normalmente.</p>
        <div class="rem-url"><input readonly value="${st.url}" id="remUrl">
          <button onclick="copiarRemoto()">Copiar</button></div>
        <img class="rem-qr" src="${qr}" alt="QR code">
        <p class="rem-hint">Aponte a câmera do celular para o QR code para abrir direto.
          ${st.fixo?"<br>Este endereço é sempre o mesmo — pode salvar nos favoritos.":
          "<br>⚠️ Este endereço muda cada vez que ligar. Configure um endereço fixo abaixo."}</p>
        <button class="wiz-btn" style="margin-top:1rem" onclick="desligarRemoto()">Desligar acesso remoto</button>
        ${st.fixo?"":`<button class="wiz-btn" style="margin-top:.5rem" onclick="configRemoto()">Configurar endereço fixo</button>`}
      </div>`;
  }else if(st.status==="iniciando"){
    body.innerHTML=`<div class="rem-load"><div class="rem-spin"></div>
      <p>Conectando… aguarde alguns segundos.</p></div>`;
    setTimeout(atualizarRemoto, 2000);
  }else if(st.status==="erro"){
    body.innerHTML=`<div class="rem-off">
      <p class="rem-erro">⚠️ ${st.erro||"Não consegui ativar o acesso remoto."}</p>
      <button class="wiz-btn primary" onclick="ligarRemoto()">Tentar de novo</button>
      <button class="wiz-btn" style="margin-top:.5rem" onclick="configRemoto()">Configurar endereço fixo</button></div>`;
  }else{
    const temFixo = st.fixo;
    body.innerHTML=`<div class="rem-off">
      <div class="rem-ic">🌐</div>
      <p class="rem-txt">Veja suas impressoras — com métricas ao vivo e câmera —
        de fora da sua rede, no celular ou em outro PC. Não precisa mexer no roteador.</p>
      ${temFixo?`<p class="rem-hint">Endereço fixo configurado:
        <b>${st.hostname}</b></p>`:
        `<p class="rem-hint">O acesso é protegido por login.</p>`}
      <button class="wiz-btn primary" onclick="ligarRemoto()">Ativar acesso remoto</button>
      <button class="wiz-btn" style="margin-top:.5rem" onclick="configRemoto()">
        ${temFixo?"Alterar endereço fixo":"Configurar endereço fixo"}</button>
    </div>`;
  }
}
async function configRemoto(){
  const body=document.getElementById("remotoBody");
  if(!body) return;
  let cfg={tem_token:false,hostname:"",auto:false};
  try{ cfg=await (await fetch("/api/remoto/config")).json(); }catch(_){}
  body.innerHTML=`
    <div class="rem-cfg">
      <p class="rem-txt">Para ter um endereço fixo (que nunca muda), cole aqui o
        <b>token</b> e o <b>endereço</b> gerados na sua conta Cloudflare.</p>
      <label>Token do túnel</label>
      <input id="remToken" type="password" placeholder="${cfg.tem_token?'•••••• (já configurado — cole para trocar)':'cole o token aqui'}">
      <label>Endereço fixo (hostname)</label>
      <input id="remHost" placeholder="cliente.seudominio.com.br" value="${cfg.hostname||''}">
      <label class="rem-check">
        <input type="checkbox" id="remAuto" ${cfg.auto?'checked':''}>
        Ligar o acesso remoto automaticamente ao abrir o sistema
      </label>
      <div class="rem-erro" id="remCfgErr" style="display:none"></div>
      <button class="wiz-btn primary" style="margin-top:1rem" onclick="salvarConfigRemoto()">Salvar</button>
      <button class="wiz-btn" style="margin-top:.5rem" onclick="atualizarRemoto()">Voltar</button>
      <p class="rem-hint" style="margin-top:1rem">Não sabe como obter o token?
        Fale com o suporte — enviamos o passo a passo.</p>
    </div>`;
}
async function salvarConfigRemoto(){
  const token=document.getElementById("remToken")?.value||"";
  const host=document.getElementById("remHost")?.value||"";
  const auto=document.getElementById("remAuto")?.checked||false;
  const err=document.getElementById("remCfgErr");
  const body={hostname:host, auto:auto};
  if(token.trim()) body.token=token.trim();   // só troca o token se digitou
  if(host && !token.trim()){
    // ok trocar só hostname/auto
  }
  try{
    const r=await fetch("/api/remoto/config",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(!d.ok){ if(err){err.style.display="block"; err.textContent=d.error||"Erro ao salvar.";} return; }
    atualizarRemoto();
  }catch(_){ if(err){err.style.display="block"; err.textContent="Erro ao salvar.";} }
}
async function ligarRemoto(){
  const body=document.getElementById("remotoBody");
  if(body) body.innerHTML=`<div class="rem-load"><div class="rem-spin"></div>
    <p>Ativando… isso pode levar até 30 segundos na primeira vez
    (baixando um componente).</p></div>`;
  try{
    await fetch("/api/remoto/ligar",{method:"POST"});
  }catch(_){}
  atualizarRemoto();
}
async function desligarRemoto(){
  try{ await fetch("/api/remoto/desligar",{method:"POST"}); }catch(_){}
  atualizarRemoto();
}
function copiarRemoto(){
  const i=document.getElementById("remUrl");
  if(i){ i.select(); document.execCommand("copy");
    const b=event.target; const t=b.textContent; b.textContent="Copiado!";
    setTimeout(()=>b.textContent=t,1500); }
}
function mostrarModalGenerico(html){
  const m=document.getElementById("genModal");
  const o=document.getElementById("genOverlay");
  if(m&&o){ m.innerHTML=html; o.classList.add("open"); }
}
function fecharModalGenerico(){
  document.getElementById("genOverlay")?.classList.remove("open");
}

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

/* ── ORÇAMENTOS ───────────────────────────────────────── */
let qState={view:"list", orc:null};

function qVazio(){
  return {id:null, cliente:{nome:"",contato:"",email:""},
    itens:[{desc:"",material:"PLA",peso_g:"",peso_unid:"g",tempo_min:"",tempo_unid:"min",valor_filamento:"",qtd:1,valor_venda:""}],
    extras:[], desconto_pct:0, desconto_rs:0, validade_dias:7, obs:"",
    status:"rascunho"};
}
function renderQuotes(){
  if(qState.view==="edit") qEditor(); else qListView();
}
async function qListView(){
  qState.view="list";
  const box=document.getElementById("quotesContent");
  if(!box) return;
  box.innerHTML=`<div class="dash-empty">Carregando…</div>`;
  let lista=[];
  try{
    const d=await (await fetch("/api/orcamentos/list")).json();
    if(d.ok) lista=d.orcamentos;
  }catch(_){}
  const badge=s=>({rascunho:'<span class="q-bd q-rasc">Rascunho</span>',
    enviado:'<span class="q-bd q-env">Enviado</span>',
    aprovado:'<span class="q-bd q-apr">Aprovado</span>',
    recusado:'<span class="q-bd q-rec">Recusado</span>'}[s]||s);
  const linhas=lista.map(o=>{
    let dt="";
    try{ dt=new Date(o.criado_em).toLocaleDateString("pt-BR"); }catch(_){}
    return `<tr>
      <td><b>${o.numero||""}</b></td>
      <td>${o.cliente||"<i style='color:var(--faint)'>sem nome</i>"}</td>
      <td>${dt}</td>
      <td style="text-align:center">${o.qtd_itens}</td>
      <td style="text-align:right"><b>${money(o.total)}</b></td>
      <td>${badge(o.status)}</td>
      <td class="q-acts">
        <button onclick="qEdit('${o.id}')" title="Abrir">✎</button>
        <a href="/api/orcamentos/pdf?id=${o.id}" target="_blank" title="PDF">📄</a>
        <button onclick="qDup('${o.id}')" title="Duplicar">⧉</button>
        <button onclick="qDel('${o.id}','${escq(o.numero||"")}')" title="Excluir">🗑</button>
      </td></tr>`;
  }).join("");

  // resumo rápido
  const aprov=lista.filter(o=>o.status==="aprovado");
  const somaAprov=aprov.reduce((a,o)=>a+(o.total||0),0);
  const pend=lista.filter(o=>o.status==="enviado");
  const somaPend=pend.reduce((a,o)=>a+(o.total||0),0);

  box.innerHTML=`
    <div class="q-top">
      <div class="q-mini"><small>Aprovados</small><b>${money(somaAprov)}</b><span>${aprov.length} orçamento(s)</span></div>
      <div class="q-mini"><small>Aguardando resposta</small><b>${money(somaPend)}</b><span>${pend.length} enviado(s)</span></div>
      <div style="flex:1"></div>
      <button class="kbtn kbtn-primary" onclick="qNew()">＋ Novo orçamento</button>
    </div>
    ${lista.length?`<div class="q-tablewrap"><table class="q-table">
      <thead><tr><th>Nº</th><th>Cliente</th><th>Data</th><th style="text-align:center">Itens</th>
        <th style="text-align:right">Total</th><th>Situação</th><th></th></tr></thead>
      <tbody>${linhas}</tbody></table></div>`
    :`<div class="dash-empty"><b>Nenhum orçamento ainda</b>Clique em "Novo orçamento" para criar o primeiro.</div>`}`;
}
function qNew(){ qState={view:"edit", orc:qVazio()}; qEditor(); }
async function qEdit(id){
  try{
    const d=await (await fetch("/api/orcamentos/get?id="+encodeURIComponent(id))).json();
    if(!d.ok){ alert(d.error||"Erro."); return; }
    qState={view:"edit", orc:d.orcamento};
    if(!qState.orc.itens || !qState.orc.itens.length) qState.orc.itens=[{desc:"",material:"PLA",peso_g:"",tempo_min:"",qtd:1,acabamento:""}];
    qEditor();
  }catch(_){ alert("Erro ao abrir."); }
}
async function qDup(id){
  const r=await fetch("/api/orcamentos/duplicar",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({id})});
  const d=await r.json();
  if(!d.ok){ alert(d.error||"Erro."); return; }
  qEdit(d.id);
}
async function qDel(id,num){
  if(!confirm(`Excluir o orçamento ${num}?`)) return;
  const r=await fetch("/api/orcamentos/delete",{method:"POST",
    headers:{"Content-Type":"application/json"},body:JSON.stringify({id})});
  const d=await r.json();
  if(!d.ok){ alert(d.error||"Erro."); return; }
  qListView();
}

function qEditor(){
  const box=document.getElementById("quotesContent");
  const o=qState.orc;
  if(!box||!o) return;
  const mats=["PLA","PETG","ABS","TPU","ASA","Nylon","Resina","Outro"];
  const itens=o.itens.map((it,i)=>`
    <tr>
      <td><input class="qi" data-f="desc" data-i="${i}" value="${escq(it.desc||"")}" placeholder="Ex.: Vaso decorativo" oninput="qTouch()"></td>
      <td><select class="qi" data-f="material" data-i="${i}" onchange="qTouch()">
        ${mats.map(mm=>`<option ${mm===(it.material||"PLA")?"selected":""}>${mm}</option>`).join("")}
      </select></td>
      <td><input class="qi qnum" data-f="peso_g" data-i="${i}" type="number" min="0" step="0.01" value="${it.peso_g??""}" placeholder="0" oninput="qTouch()"></td>
      <td><select class="qi q-tempo-u" data-f="peso_unid" data-i="${i}" onchange="qTouch()">
        <option value="g" ${(it.peso_unid||"g")==="g"?"selected":""}>g</option>
        <option value="kg" ${it.peso_unid==="kg"?"selected":""}>kg</option>
      </select></td>
      <td><div class="q-tempo">
        <input class="qi qnum" data-f="tempo_min" data-i="${i}" type="number" min="0" step="0.1" value="${it.tempo_min??""}" placeholder="0" oninput="qTouch()">
        <select class="qi q-tempo-u" data-f="tempo_unid" data-i="${i}" onchange="qTouch()">
          <option value="min" ${(it.tempo_unid||"min")==="min"?"selected":""}>min</option>
          <option value="h" ${it.tempo_unid==="h"?"selected":""}>h</option>
        </select>
      </div></td>
      <td><input class="qi qnum" data-f="valor_filamento" data-i="${i}" type="number" min="0" step="0.01" value="${it.valor_filamento??""}" placeholder="ex: 100" oninput="qTouch()" title="Preço do rolo por kg (ex.: rolo de 1kg custa R$ 100 → digite 100)"></td>
      <td><input class="qi qnum" data-f="qtd" data-i="${i}" type="number" min="1" step="1" value="${it.qtd??1}" oninput="qTouch()"></td>
      <td><input class="qi qnum q-venda" data-f="valor_venda" data-i="${i}" type="number" min="0" step="0.01" value="${it.valor_venda??""}" placeholder="0,00" oninput="qTouch()" title="Preço que o cliente paga por unidade"></td>
      <td class="q-sub" id="qsub${i}">—</td>
      <td><button class="q-x" onclick="qDelItem(${i})" title="Remover">✕</button></td>
    </tr>`).join("");
  const extras=(o.extras||[]).map((e,i)=>`
    <tr>
      <td><input class="qe" data-f="desc" data-i="${i}" value="${escq(e.desc||"")}" placeholder="Ex.: Frete, embalagem" oninput="qTouch()"></td>
      <td><input class="qe qnum" data-f="valor" data-i="${i}" type="number" min="0" step="0.01" value="${e.valor??""}" placeholder="0,00" oninput="qTouch()"></td>
      <td><button class="q-x" onclick="qDelExtra(${i})" title="Remover">✕</button></td>
    </tr>`).join("");

  box.innerHTML=`
    <div class="q-edithead">
      <button class="kbtn" onclick="qListView()">← Voltar</button>
      <div class="q-num">${o.numero?o.numero:"Novo orçamento"}</div>
      <div style="flex:1"></div>
      ${o.id?`<select class="q-status" id="qStatus" onchange="qSetStatus()">
        ${["rascunho","enviado","aprovado","recusado"].map(s=>
          `<option value="${s}" ${s===(o.status||"rascunho")?"selected":""}>${
            {rascunho:"Rascunho",enviado:"Enviado",aprovado:"Aprovado",recusado:"Recusado"}[s]}</option>`).join("")}
      </select>`:""}
      ${o.id?`<a class="kbtn" href="/api/orcamentos/pdf?id=${o.id}" target="_blank">📄 PDF</a>`:""}
      <button class="kbtn kbtn-primary" onclick="qSave()">💾 Salvar</button>
    </div>

    <div class="q-box q-databox">
      <label class="q-datalbl">📅 Data do orçamento
        <input type="date" id="qData" value="${(o.criado_em||"").slice(0,10)}" onchange="qTouch()">
      </label>
    </div>

    <div class="q-box">
      <h3>Cliente</h3>
      <div class="q-cli">
        <label>Nome<input id="qcNome" value="${escq((o.cliente||{}).nome||"")}" placeholder="Nome do cliente"></label>
        <label>Contato<input id="qcContato" value="${escq((o.cliente||{}).contato||"")}" placeholder="Telefone / WhatsApp"></label>
        <label>E-mail<input id="qcEmail" value="${escq((o.cliente||{}).email||"")}" placeholder="opcional"></label>
      </div>
    </div>

    <div class="q-box">
      <h3>Peças</h3>
      <div class="q-tablewrap"><table class="q-table q-itens">
        <thead><tr>
          <th style="min-width:160px">Descrição</th><th>Material</th>
          <th title="Peso do filamento por unidade" colspan="2">Peso</th>
          <th title="Tempo de impressão por unidade">Tempo</th>
          <th title="Preço do rolo de filamento por quilo (ex.: rolo de 1kg por R$ 100 = digite 100). O sistema calcula o custo da peça pelo peso.">Filamento (R$/kg)</th>
          <th>Qtd</th>
          <th title="Preço que o cliente paga por unidade">Valor de venda (R$)</th>
          <th style="text-align:right">Subtotal</th><th></th>
        </tr></thead>
        <tbody>${itens}</tbody>
      </table></div>
      <button class="kbtn" onclick="qAddItem()" style="margin-top:.7rem">＋ Adicionar peça</button>
    </div>

    <div class="q-box">
      <h3>Serviços adicionais</h3>
      ${extras?`<div class="q-tablewrap"><table class="q-table q-extras">
        <thead><tr><th>Descrição</th><th style="width:140px">Valor (R$)</th><th></th></tr></thead>
        <tbody>${extras}</tbody></table></div>`
        :`<p style="color:var(--faint);font-size:.85rem;margin:0">Nenhum serviço adicional.</p>`}
      <button class="kbtn" onclick="qAddExtra()" style="margin-top:.7rem">＋ Adicionar serviço</button>
    </div>

    <div class="dash-cols">
      <div class="q-box">
        <h3>Condições</h3>
        <div class="q-cond">
          <label>Desconto (%)<input id="qDescPct" type="number" min="0" step="0.1" value="${o.desconto_pct||0}" oninput="qTouch()"></label>
          <label>Desconto (R$)<input id="qDescRs" type="number" min="0" step="0.01" value="${o.desconto_rs||0}" oninput="qTouch()"></label>
          <label>Validade (dias)<input id="qValidade" type="number" min="1" step="1" value="${o.validade_dias||7}"></label>
        </div>
        <label class="q-obs">Observações
          <textarea id="qObs" rows="3" placeholder="Prazo de entrega, forma de pagamento…">${(o.obs||"").replace(/</g,"&lt;")}</textarea>
        </label>
      </div>
      <div class="q-box q-totbox">
        <h3>Total</h3>
        <div id="qTotais" class="q-totais"><div class="dash-empty">Calculando…</div></div>
      </div>
    </div>`;
  qRecalc();
}
function qAddItem(){
  qCollect();
  qState.orc.itens.push({desc:"",material:"PLA",peso_g:"",peso_unid:"g",tempo_min:"",tempo_unid:"min",valor_filamento:"",qtd:1,valor_venda:""});
  qEditor();
}
function qDelItem(i){
  qCollect();
  qState.orc.itens.splice(i,1);
  if(!qState.orc.itens.length) qState.orc.itens=[{desc:"",material:"PLA",peso_g:"",tempo_min:"",qtd:1,acabamento:""}];
  qEditor();
}
function qAddExtra(){
  qCollect();
  (qState.orc.extras=qState.orc.extras||[]).push({desc:"",valor:""});
  qEditor();
}
function qDelExtra(i){
  qCollect();
  qState.orc.extras.splice(i,1);
  qEditor();
}
/* lê os campos da tela para o objeto */
function qCollect(){
  const o=qState.orc; if(!o) return o;
  const g=id=>{const e=document.getElementById(id);return e?e.value:"";};
  o.cliente={nome:g("qcNome"),contato:g("qcContato"),email:g("qcEmail")};
  document.querySelectorAll(".qi").forEach(el=>{
    const i=+el.dataset.i, f=el.dataset.f;
    if(o.itens[i]) o.itens[i][f]=el.value;
  });
  o.extras=o.extras||[];
  document.querySelectorAll(".qe").forEach(el=>{
    const i=+el.dataset.i, f=el.dataset.f;
    if(o.extras[i]) o.extras[i][f]=el.value;
  });
  o.desconto_pct=parseFloat(g("qDescPct"))||0;
  o.desconto_rs=parseFloat(g("qDescRs"))||0;
  o.validade_dias=parseInt(g("qValidade"),10)||7;
  o.obs=g("qObs");
  const dt=g("qData");
  if(dt) o.data_orc=dt;   // data escolhida pelo usuário (YYYY-MM-DD)
  return o;
}
let qTimer=null;
function qTouch(){ clearTimeout(qTimer); qTimer=setTimeout(qRecalc,250); }
async function qRecalc(){
  const o=qCollect(); if(!o) return;
  try{
    const r=await fetch("/api/orcamentos/preview",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(o)});
    const d=await r.json();
    if(!d.ok) return;
    const t=d.totais;
    (t.itens||[]).forEach((ci,i)=>{
      const el=document.getElementById("qsub"+i);
      if(el) el.textContent=money(ci.subtotal);
    });
    const box=document.getElementById("qTotais");
    if(box) box.innerHTML=`
      <div class="qt-l"><span>Peças</span><b>${money(t.soma_itens)}</b></div>
      ${t.extras?`<div class="qt-l"><span>Serviços adicionais</span><b>${money(t.extras)}</b></div>`:""}
      ${t.desconto?`<div class="qt-l qt-desc"><span>Desconto</span><b>− ${money(t.desconto)}</b></div>`:""}
      <div class="qt-total"><span>TOTAL (cliente paga)</span><b>${money(t.total)}</b></div>
      <div class="qt-margem">
        <div class="qt-mrow"><span>Seu custo</span><b>${money(t.custo_total)}</b></div>
        <div class="qt-mrow qt-lucro"><span>Seu lucro</span><b>${money(t.lucro_total)} · ${t.margem_pct}%</b></div>
        <div class="qt-mhint">🔒 Só você vê esta margem. O cliente recebe apenas o valor de venda.</div>
      </div>
      <div class="qt-info">⚖ ${t.peso_total_g} g de filamento · ⏱ ${t.horas_total} h de impressão</div>`;
  }catch(_){}
}
async function qSave(){
  const o=qCollect();
  if(!o.cliente.nome.trim() && !confirm("Salvar sem o nome do cliente?")) return;
  try{
    const r=await fetch("/api/orcamentos/save",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(o)});
    const d=await r.json();
    if(!d.ok){ alert(d.error||"Erro ao salvar."); return; }
    qState.orc.id=d.id; qState.orc.numero=d.numero;
    alert("Orçamento "+d.numero+" salvo!");
    qEditor();
  }catch(_){ alert("Erro ao salvar."); }
}
async function qSetStatus(){
  const sel=document.getElementById("qStatus");
  if(!sel||!qState.orc.id) return;
  const r=await fetch("/api/orcamentos/status",{method:"POST",
    headers:{"Content-Type":"application/json"},
    body:JSON.stringify({id:qState.orc.id,status:sel.value})});
  const d=await r.json();
  if(!d.ok){ alert(d.error||"Erro."); return; }
  qState.orc.status=sel.value;
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
  montarRelatorioEstoque();
}

/* ── Relatório de consumo de filamento (dentro de Relatórios) ── */
let relEstFiltro={periodo:"mes", printers:[], item_id:""};
async function montarRelatorioEstoque(){
  const host=document.getElementById("reportsContent");
  if(!host) return;
  let sec=document.getElementById("relEstoqueSec");
  if(!sec){
    sec=document.createElement("div");
    sec.id="relEstoqueSec";
    sec.className="rel-est-sec";
    host.appendChild(sec);
  }
  // opções de impressora e de filamento
  let impressoras=[], itens=[];
  try{ impressoras=Object.keys(lastData||{}); }catch(_){}
  try{ itens=(await (await fetch("/api/estoque")).json()).itens||[]; }catch(_){}

  const periodos=[["dia","Hoje"],["semana","Semana"],["mes","Mês"],["ano","Ano"]];
  const btnsPeriodo=periodos.map(([v,l])=>
    `<button class="rel-chip ${relEstFiltro.periodo===v?'on':''}" onclick="relEstSetPeriodo('${v}')">${l}</button>`).join("");
  const optImpr=impressoras.map(n=>
    `<label class="rel-imp"><input type="checkbox" value="${escapeHtml(n)}" ${relEstFiltro.printers.includes(n)?'checked':''} onchange="relEstToggleImp('${escapeHtml(n).replace(/'/g,"\\'")}')"> ${escapeHtml(n)}</label>`).join("");
  const optItens=itens.map(i=>
    `<option value="${i.id}" ${String(relEstFiltro.item_id)===String(i.id)?'selected':''}>${escapeHtml(i.marca)} · ${escapeHtml(i.tipo)} · ${escapeHtml(i.cor)}</option>`).join("");

  sec.innerHTML=`
    <div class="q-box">
      <h3>📦 Consumo de filamento</h3>
      <div class="rel-filtros">
        <div class="rel-fg"><span class="rel-lbl">Período</span><div class="rel-chips">${btnsPeriodo}</div></div>
        <div class="rel-fg"><span class="rel-lbl">Filamento</span>
          <select id="relEstItem" onchange="relEstSetItem(this.value)" class="q-status">
            <option value="">Todos os filamentos</option>${optItens}
          </select></div>
      </div>
      ${impressoras.length?`<div class="rel-fg" style="margin-top:1rem">
        <span class="rel-lbl">Impressoras <small style="color:var(--faint)">(nenhuma marcada = todas)</small></span>
        <div class="rel-imps">${optImpr}</div></div>`:""}
      <div id="relEstResultado" style="margin-top:1.4rem"><div class="dash-empty">Carregando…</div></div>
    </div>`;
  carregarRelatorioEstoque();
}
function relEstSetPeriodo(p){ relEstFiltro.periodo=p; montarRelatorioEstoque(); }
function relEstSetItem(v){ relEstFiltro.item_id=v; carregarRelatorioEstoque(); }
function relEstToggleImp(n){
  const i=relEstFiltro.printers.indexOf(n);
  if(i>=0) relEstFiltro.printers.splice(i,1); else relEstFiltro.printers.push(n);
  carregarRelatorioEstoque();
}
function _periodoDatas(p){
  const agora=new Date(); let ini=new Date();
  if(p==="dia"){ ini.setHours(0,0,0,0); }
  else if(p==="semana"){ ini.setDate(agora.getDate()-7); }
  else if(p==="mes"){ ini.setMonth(agora.getMonth()-1); }
  else if(p==="ano"){ ini.setFullYear(agora.getFullYear()-1); }
  return {inicio:ini.toISOString(), fim:agora.toISOString()};
}
async function carregarRelatorioEstoque(){
  const out=document.getElementById("relEstResultado");
  if(!out) return;
  const {inicio,fim}=_periodoDatas(relEstFiltro.periodo);
  const q=new URLSearchParams({inicio,fim});
  if(relEstFiltro.printers.length) q.set("printers",relEstFiltro.printers.join(","));
  if(relEstFiltro.item_id) q.set("item_id",relEstFiltro.item_id);
  let rep;
  try{ rep=await (await fetch("/api/estoque/relatorio?"+q.toString())).json(); }
  catch(_){ out.innerHTML=`<div class="dash-empty">Erro ao carregar.</div>`; return; }
  if(!rep.itens||!rep.itens.length){
    out.innerHTML=`<div class="dash-empty">Nenhum consumo registrado neste período.</div>`;
    return;
  }
  const linhas=rep.itens.map(i=>{
    const sw=i.cor_hex?`<span class="est-sw" style="background:${i.cor_hex}"></span>`:"";
    return `<tr>
      <td>${sw}${escapeHtml(i.marca)} ${escapeHtml(i.tipo)} ${escapeHtml(i.cor)}</td>
      <td class="mono" style="text-align:right">${i.gramas.toFixed(0)} g</td>
      <td class="mono" style="text-align:right">${i.kg.toFixed(3)} kg</td>
      <td class="mono" style="text-align:right">R$ ${i.custo.toFixed(2)}</td>
      <td class="mono" style="text-align:right">${i.impressoes}</td>
    </tr>`;
  }).join("");
  out.innerHTML=`
    <div class="rel-resumo">
      <div class="rel-rc"><small>Total usado</small><b>${rep.total_gramas.toFixed(0)} g</b><span>${rep.total_kg.toFixed(3)} kg</span></div>
      <div class="rel-rc"><small>Custo total</small><b>R$ ${rep.total_custo.toFixed(2)}</b><span>em filamento</span></div>
      <div class="rel-rc"><small>Impressões</small><b>${rep.total_impressoes}</b><span>no período</span></div>
    </div>
    <div class="q-tablewrap" style="margin-top:1rem"><table class="q-table">
      <thead><tr><th>Filamento</th><th style="text-align:right">Gramas</th>
        <th style="text-align:right">Kg</th><th style="text-align:right">Custo</th>
        <th style="text-align:right">Impressões</th></tr></thead>
      <tbody>${linhas}</tbody>
    </table></div>`;
}
/* ══════════════ ESTOQUE DE FILAMENTO ══════════════ */
let estoqueCache=[];
async function renderEstoque(){
  const box=document.getElementById("estoqueContent");
  if(!box) return;
  box.innerHTML=`<div class="dash-empty">Carregando estoque…</div>`;
  let itens=[];
  try{
    const d=await (await fetch("/api/estoque")).json();
    itens=d.itens||[];
  }catch(_){ box.innerHTML=`<div class="dash-empty">Erro ao carregar o estoque.</div>`; return; }
  estoqueCache=itens;

  // resumo: total de kg e alertas
  const totalKg=itens.reduce((s,i)=>s+i.saldo_g,0)/1000;
  const alertas=itens.filter(i=>i.alerta);

  const linhas=itens.length?itens.map(i=>{
    const cls=i.alerta?("est-"+i.alerta):"";
    const badge=alertaBadge(i.alerta);
    const sw=i.cor_hex?`<span class="est-sw" style="background:${i.cor_hex}"></span>`:"";
    return `<tr class="${cls}">
      <td>${sw}${escapeHtml(i.marca)}</td>
      <td>${escapeHtml(i.tipo)}</td>
      <td>${escapeHtml(i.cor)}</td>
      <td class="mono" style="text-align:right"><b>${i.saldo_kg.toFixed(3)}</b> kg</td>
      <td class="mono" style="text-align:right">R$ ${i.preco_kg.toFixed(2)}</td>
      <td>${badge}</td>
      <td style="text-align:right">
        <button class="q-x" title="Editar" onclick="estoqueEditar(${i.id})">✎</button>
        <button class="q-x" title="Remover" onclick="estoqueRemover(${i.id})">✕</button>
      </td></tr>`;
  }).join(""):`<tr><td colspan="7" style="text-align:center;color:var(--faint);padding:2rem">
      Nenhum filamento cadastrado ainda. Adicione o primeiro abaixo.</td></tr>`;

  const avisos=alertas.length?`
    <div class="est-alertas">
      ${alertas.map(i=>`<div class="est-aviso est-${i.alerta}">
        ${alertaIcone(i.alerta)} <b>${escapeHtml(i.marca)} ${escapeHtml(i.tipo)} ${escapeHtml(i.cor)}</b>:
        ${i.saldo_kg.toFixed(2)}kg restantes ${alertaTexto(i.alerta)}</div>`).join("")}
    </div>`:"";

  box.innerHTML=`
    <div class="est-top">
      <div class="q-mini"><small>Total em estoque</small><b class="mono">${totalKg.toFixed(2)} kg</b>
        <span>${itens.length} filamento(s)</span></div>
      <div class="q-mini"><small>Em alerta</small><b class="mono" style="color:${alertas.length?'var(--warn)':'var(--done)'}">${alertas.length}</b>
        <span>${alertas.length?'precisam de atenção':'tudo ok'}</span></div>
    </div>
    ${avisos}
    <div class="q-box">
      <h3>Filamentos em estoque</h3>
      <div class="q-tablewrap"><table class="q-table">
        <thead><tr><th>Marca</th><th>Tipo</th><th>Cor</th>
          <th style="text-align:right">Saldo</th><th style="text-align:right">Preço/kg</th>
          <th>Situação</th><th></th></tr></thead>
        <tbody>${linhas}</tbody>
      </table></div>
    </div>
    <div class="q-box">
      <h3>Adicionar / repor filamento</h3>
      <div class="est-form">
        <label>Marca<input id="estMarca" placeholder="Ex: Voolt" list="estMarcas"></label>
        <label>Tipo<input id="estTipo" placeholder="Ex: PLA, PETG" list="estTipos"></label>
        <label>Cor<input id="estCor" placeholder="Ex: Vermelho"></label>
        <label>Cor (visual)<input id="estCorHex" type="color" value="#4f8cff"></label>
        <label>Quantidade (kg)<input id="estKg" type="number" min="0" step="0.001" placeholder="1.000"></label>
        <label>Preço do rolo (R$/kg)<input id="estPreco" type="number" min="0" step="0.01" placeholder="120,00"></label>
      </div>
      <datalist id="estMarcas">${[...new Set(itens.map(i=>i.marca))].map(m=>`<option value="${escapeHtml(m)}">`).join("")}</datalist>
      <datalist id="estTipos"><option value="PLA"><option value="PETG"><option value="ABS"><option value="TPU"><option value="ASA"><option value="PLA+"></datalist>
      <button class="kbtn" style="margin-top:1rem" onclick="estoqueSalvar()">＋ Adicionar ao estoque</button>
      <p class="est-hint">Se a marca+tipo+cor já existir, a quantidade é somada ao saldo atual.</p>
    </div>`;
}
function alertaBadge(a){
  if(a==="negativo") return `<span class="est-bd est-bd-neg">Negativo</span>`;
  if(a==="critico") return `<span class="est-bd est-bd-crit">Crítico</span>`;
  if(a==="baixo") return `<span class="est-bd est-bd-baixo">Baixo</span>`;
  if(a==="atencao") return `<span class="est-bd est-bd-at">Atenção</span>`;
  return `<span class="est-bd est-bd-ok">Ok</span>`;
}
function alertaIcone(a){ return a==="negativo"?"🔴":a==="critico"?"🔴":a==="baixo"?"🟠":"🟡"; }
function alertaTexto(a){
  return a==="negativo"?"— estoque NEGATIVO!":a==="critico"?"— quase acabando (abaixo de 1kg)":
         a==="baixo"?"— nível baixo (abaixo de 5kg)":"— fique de olho (abaixo de 10kg)";
}
async function estoqueSalvar(){
  const g=id=>document.getElementById(id);
  const body={marca:g("estMarca").value,tipo:g("estTipo").value,cor:g("estCor").value,
    cor_hex:g("estCorHex").value,kg:g("estKg").value,preco_kg:g("estPreco").value};
  if(!body.marca||!body.tipo||!body.cor){ alert("Preencha marca, tipo e cor."); return; }
  const d=await (await fetch("/api/estoque/add",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify(body)})).json();
  if(d.ok) renderEstoque();
  else alert(d.error||"Erro ao salvar.");
}
async function estoqueRemover(id){
  const it=estoqueCache.find(x=>x.id===id);
  if(!confirm(`Remover ${it?it.marca+" "+it.tipo+" "+it.cor:"este filamento"} do estoque?`)) return;
  const d=await (await fetch("/api/estoque/remover",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({id})})).json();
  if(d.ok) renderEstoque(); else alert(d.error||"Erro.");
}
function estoqueEditar(id){
  const it=estoqueCache.find(x=>x.id===id); if(!it) return;
  const kg=prompt(`Ajustar saldo de ${it.marca} ${it.tipo} ${it.cor} (kg):`, it.saldo_kg.toFixed(3));
  if(kg===null) return;
  const preco=prompt("Preço do rolo (R$/kg):", it.preco_kg.toFixed(2));
  if(preco===null) return;
  fetch("/api/estoque/editar",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({id,marca:it.marca,tipo:it.tipo,cor:it.cor,cor_hex:it.cor_hex,
      saldo_kg:kg,preco_kg:preco})}).then(r=>r.json()).then(d=>{
    if(d.ok) renderEstoque(); else alert(d.error||"Erro.");
  });
}
function escapeHtml(s){ return String(s||"").replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }

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

  // carrega o estoque para o usuário escolher o filamento
  let estoque=[];
  try{ estoque=(await (await fetch("/api/estoque")).json()).itens||[]; }catch(_){}
  const temEstoque=estoque.length>0;

  // opções de filamento agrupadas: "Marca · Tipo · Cor (saldo)"
  const opcoes=estoque.map(i=>`<option value="${i.id}">${escapeHtml(i.marca)} · ${escapeHtml(i.tipo)} · ${escapeHtml(i.cor)} (${i.saldo_kg.toFixed(2)}kg)</option>`).join("");

  const blocoEstoque=temEstoque?`
      <div class="cf-row"><label>Filamento (do estoque)</label>
        <select id="cf_estoque" onchange="custoPreview()" style="flex:1">
          <option value="">— escolha —</option>${opcoes}
        </select></div>
      <div class="cf-row"><label>Peso usado</label>
        <input type="number" id="cf_peso" value="" step="1" min="0" placeholder="ex: 45"
          oninput="custoPreview()"><span class="cf-u">g</span></div>
      <label class="cf-desc"><input type="checkbox" id="cf_descontar" checked> Descontar do estoque</label>
  `:`
      <div class="ca-hint" style="margin-bottom:.8rem;padding:.7rem .9rem;background:rgba(79,140,255,.08);border-radius:9px">
        💡 Cadastre seus filamentos em <b>Estoque</b> para escolher aqui e ter o controle automático.
      </div>
      <div class="cf-row"><label>Material</label>
        <select id="cf_material"><option>PLA</option><option>PETG</option><option>ABS</option><option>TPU</option></select></div>
      <div class="cf-row"><label>Preço do filamento</label>
        <input type="number" id="cf_preco_kg" value="${cfg.preco_kg}" step="1" min="0" oninput="custoPreview()"><span class="cf-u">R$/kg</span></div>
      <div class="cf-row"><label>Peso da peça</label>
        <input type="number" id="cf_peso" value="" step="1" min="0" placeholder="ex: 45" oninput="custoPreview()"><span class="cf-u">g</span></div>
  `;

  document.getElementById("custoModal").innerHTML=`
    <div class="calc-head"><b>💰 Custo da impressão</b>
      <div class="mclose" onclick="fecharCusto()">✕</div></div>
    <div class="custo-form">
      <div class="ca-printer" style="margin-bottom:.15rem">${disp}</div>
      <div class="ca-file" style="margin-bottom:1rem">${jobKey(st)}</div>
      ${blocoEstoque}
      <div class="cf-row"><label>Tempo de impressão</label>
        <input type="number" id="cf_min" value="${minutos}" step="1" min="0"
          oninput="custoPreview()"><span class="cf-u">min</span></div>
      <div class="cf-row"><label>Custo da energia</label>
        <input type="number" id="cf_kwh" value="${cfg.preco_kwh}" step="0.01" min="0"
          oninput="custoPreview()"><span class="cf-u">R$/kWh</span></div>
      <div class="cf-out" id="cf_out">Informe o peso para calcular.</div>
      <div class="ca-btns" style="margin-top:1rem">
        <button class="ca-no" onclick="pularCusto()">Pular</button>
        <button class="ca-yes" onclick="salvarCusto()">Salvar custo</button>
      </div>
    </div>`;
  window._custoEstoque=estoque;
  custoPreview();
}
function custoPreview(){
  const g=id=>parseFloat((document.getElementById(id)||{}).value)||0;
  const peso=g("cf_peso"), min=g("cf_min"), kwh=g("cf_kwh");
  const out=document.getElementById("cf_out");
  if(!out) return;
  // preço do kg: do estoque escolhido, ou do campo manual
  let pkg=0;
  const selEst=document.getElementById("cf_estoque");
  if(selEst && selEst.value){
    const it=(window._custoEstoque||[]).find(x=>String(x.id)===selEst.value);
    if(it) pkg=it.preco_kg;
  }else{
    pkg=g("cf_preco_kg");
  }
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
  const selEst=document.getElementById("cf_estoque");
  // Caminho 1: usando o estoque
  if(selEst){
    if(!selEst.value){ alert("Escolha o filamento do estoque (ou cadastre em Estoque)."); return; }
    const descontar=document.getElementById("cf_descontar")?.checked;
    const it=(window._custoEstoque||[]).find(x=>String(x.id)===selEst.value);
    if(descontar){
      const d=await (await fetch("/api/estoque/descontar",{method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({id:parseInt(selEst.value),gramas:peso,printer:name,obs:jobKey(st)})})).json();
      if(!d.ok){ alert(d.error||"Erro ao descontar do estoque."); return; }
      // registra o custo do job também
      await fetch("/api/custo/set",{method:"POST",headers:{"Content-Type":"application/json"},
        body:JSON.stringify({name,file:jobKey(st),material:it?it.tipo:"",
          preco_kg:it?it.preco_kg:0,peso_g:peso,minutos:g("cf_min"),preco_kwh:g("cf_kwh")})});
      printCosts[name]={file:jobKey(st),custo:(d.info?d.info.custo:0),
        material:it?`${it.marca} ${it.tipo} ${it.cor}`:"",peso_g:peso,skip:false};
      let msg="Custo salvo e descontado do estoque.";
      if(d.info&&d.info.alerta) msg+="\n\n⚠️ Atenção: "+(it?`${it.marca} ${it.tipo} ${it.cor}`:"filamento")+" está com estoque "+
        (d.info.alerta==="negativo"?"NEGATIVO":d.info.alerta==="critico"?"crítico (abaixo de 1kg)":
         d.info.alerta==="baixo"?"baixo (abaixo de 5kg)":"em atenção (abaixo de 10kg)")+".";
      fecharCusto();
      if(lastData) render(lastData, lastOrder);
      if(d.info&&d.info.alerta) alert(msg);
      return;
    }
    // não descontar: só registra o custo pelo preço do item
    const it2=it||{preco_kg:0,tipo:""};
    const body={name,file:jobKey(st),material:it2.tipo,preco_kg:it2.preco_kg,
      peso_g:peso,minutos:g("cf_min"),preco_kwh:g("cf_kwh")};
    const r=await fetch("/api/custo/set",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(!d.ok){ alert(d.error||"Erro."); return; }
    printCosts[name]={file:body.file,custo:d.custo,material:it2.tipo,peso_g:peso,skip:false};
    fecharCusto();
    if(lastData) render(lastData, lastOrder);
    return;
  }
  // Caminho 2 (sem estoque cadastrado): modo manual antigo
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
  document.getElementById("calcModal").innerHTML=`
    <div class="calc-head"><b>🧮 Calculadora de custo</b></div>
    <div class="calc-body">
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
    let fp="", pacote="";
    try{
      const r=await fetch("/api/licenca/info");
      const d=await r.json();
      fp=d.fingerprint||""; pacote=d.pacote||"";
    }catch(_){}
    msg=`Olá! Gostaria de comprar a licença do FarmSync.`
      + (fp?`\n\nCódigo da Máquina: ${fp}`:"")
      + (pacote?`\n\nCódigo de ativação (copie tudo):\n${pacote}`:"");
  }else{
    msg="Olá! Preciso de suporte com o FarmSync.";
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
  const saved=localStorage.getItem("3dwork_viewmode")||localStorage.getItem("hub3d_viewmode");
  if(saved){ const i=VIEW_MODES.findIndex(m=>m.id===saved); if(i>=0) viewModeIdx=i; }
}catch(_){}

function applyViewMode(){
  const m=VIEW_MODES[viewModeIdx];
  const main=document.getElementById("grid");
  VIEW_MODES.forEach(v=>main.classList.remove("view-"+v.id));
  main.classList.add("view-"+m.id);
  const btn=document.getElementById("viewModeBtn");
  if(btn) btn.textContent=m.label;
  try{ localStorage.setItem("3dwork_viewmode", m.id); }catch(_){}
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
  const saved=localStorage.getItem("3dwork_cols")??localStorage.getItem("hub3d_cols");
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
  try{ localStorage.setItem("3dwork_cols", String(n)); }catch(_){}
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
    if(currentPage==="dashboard") renderDash(false);
  }catch(_){}}
  ws.onclose=()=>{document.getElementById("dot").classList.remove("live");
    document.getElementById("connlbl").textContent="reconectando…"; setTimeout(connect,2000);}
}
connect();
applyViewMode();
applyCols();
nav("dashboard");   // começa no Dashboard
checkUpdate();      // verifica se há versão nova (silencioso se não houver)
avisarSenhaPadrao();  // lembra de trocar a senha, se ainda for a padrão
avisarVencimento();   // avisa se a licença está perto de vencer

/* ── Aviso de licença perto de vencer ─────────────────── */
async function avisarVencimento(){
  try{
    const d=await (await fetch("/api/licenca/info")).json();
    const dias=d.dias_restantes;
    if(dias===null||dias===undefined) return;   // vitalícia
    if(dias>30) return;                          // ainda longe
    if(document.getElementById("vencBanner")) return;
    const bar=document.createElement("div");
    bar.id="vencBanner"; bar.className="update-banner senha-banner";
    let txt;
    if(dias<0) txt=`⛔ Sua licença venceu. Renove para continuar usando o sistema.`;
    else if(dias===0) txt=`⏰ Sua licença vence hoje. Renove para não perder o acesso.`;
    else txt=`⏰ Sua licença vence em ${dias} dia(s). Fale conosco para renovar.`;
    bar.innerHTML=`<span>${txt}</span>
      <div class="ub-btns">
        <button class="ub-later" onclick="document.getElementById('vencBanner').remove()">Depois</button>
        <button class="ub-now" onclick="abrirWhatsapp('licenca')">Renovar</button>
      </div>`;
    const content=document.querySelector(".content");
    const topbar=document.querySelector(".topbar");
    if(content && topbar) content.insertBefore(bar, topbar.nextSibling);
  }catch(_){}
}

/* ── Aviso de senha padrão ────────────────────────────── */
function avisarSenhaPadrao(){
  if("__SENHA_PADRAO__"!=="1") return;
  if(document.getElementById("senhaBanner")) return;
  const bar=document.createElement("div");
  bar.id="senhaBanner"; bar.className="update-banner senha-banner";
  bar.innerHTML=`<span>🔓 Você está usando a senha padrão (<b>admin</b>).
    Qualquer pessoa na sua rede pode acessar o painel.</span>
    <div class="ub-btns">
      <button class="ub-later" onclick="document.getElementById('senhaBanner').remove()">Depois</button>
      <button class="ub-now" onclick="nav('settings');document.getElementById('senhaBanner').remove()">Trocar senha</button>
    </div>`;
  const content=document.querySelector(".content");
  const topbar=document.querySelector(".topbar");
  if(content && topbar) content.insertBefore(bar, topbar.nextSibling);
}

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
  if(!confirm("Atualizar o sistema para a versão "+((updateInfo&&updateInfo.nova)||"nova")+"?")) return;
  const b=document.getElementById("updateBanner");
  if(b) b.innerHTML=`<span>⏳ Baixando e aplicando a atualização…</span>`;
  try{
    const d=await (await fetch("/api/update/apply",{method:"POST"})).json();
    if(d.ok){
      mostrarUpdateConcluido(d.nova);
    }else{
      if(b) b.innerHTML=`<span>⚠️ ${d.erro||"Falha na atualização."}</span>
        <div class="ub-btns"><button class="ub-later" onclick="fecharBanner()">Fechar</button></div>`;
    }
  }catch(_){
    if(b) b.innerHTML=`<span>⚠️ Erro de conexão ao atualizar.</span>
      <div class="ub-btns"><button class="ub-later" onclick="fecharBanner()">Fechar</button></div>`;
  }
}
function mostrarUpdateConcluido(nova){
  // tela clara e infalível: pede para reiniciar o computador. Como o sistema
  // inicia junto com o Windows, depois do reinício ele volta sozinho na versão
  // nova — sem depender de fechar processo, porta ou reinício automático.
  const ov=document.createElement("div");
  ov.id="updateDoneOverlay";
  ov.style.cssText="position:fixed;inset:0;background:#0a0e16;z-index:99999;"+
    "display:flex;flex-direction:column;align-items:center;justify-content:center;gap:1.3rem;color:#cdd6e4;text-align:center;padding:2rem";
  ov.innerHTML=`
    <div style="font-size:3.4rem">🎉</div>
    <div style="font-size:1.5rem;font-weight:700">Parabéns, sistema atualizado para a versão ${nova||"nova"}!</div>
    <div style="font-size:1.1rem;color:#e8edf5;max-width:460px;line-height:1.6;font-weight:600">
      Reinicie o computador para que a atualização seja aplicada!</div>
    <div style="font-size:.85rem;color:#7a8699;max-width:460px;line-height:1.5">
      Quando ligar de novo, o sistema abre sozinho já atualizado. Pode reiniciar
      agora ou quando for melhor para você — até lá, o sistema continua funcionando.</div>`;
  document.body.appendChild(ov);
}
async function _naoUsarMais_reiniciarEReconectar(){
  // dispara o reinício no servidor (ele encerra e sobe de novo)
  try{ fetch("/api/update/reiniciar",{method:"POST"}); }catch(_){}
  // mostra uma tela de "reconectando" e tenta voltar sozinho
  const ov=document.createElement("div");
  ov.id="reloadOverlay";
  ov.style.cssText="position:fixed;inset:0;background:#0a0e16;z-index:99999;"+
    "display:flex;flex-direction:column;align-items:center;justify-content:center;gap:1.2rem;color:#cdd6e4;text-align:center;padding:2rem";
  ov.innerHTML=`<div class="rem-spin"></div>
    <div style="font-size:1.05rem">Aplicando a nova versão…</div>
    <div style="font-size:.85rem;color:#7a8699;max-width:340px;line-height:1.5">
      O sistema está reiniciando. Isso leva alguns segundos.</div>
    <button onclick="location.reload()" style="margin-top:.6rem;background:rgba(79,140,255,.15);
      border:1px solid rgba(79,140,255,.4);color:#8db4ff;border-radius:8px;
      padding:.55rem 1.1rem;cursor:pointer;font-family:inherit;font-size:.85rem">
      Recarregar agora</button>`;
  document.body.appendChild(ov);
  // espera o servidor cair e voltar, então recarrega a página
  let tentativas=0, caiu=false;
  const tentar=async()=>{
    tentativas++;
    try{
      const r=await fetch("/api/health",{cache:"no-store"});
      // só recarrega depois de detectar que o servidor CHEGOU A CAIR
      // (evita reconectar no processo antigo, que ainda respondia)
      if(r.ok){
        if(caiu){ location.reload(); return; }
      }
    }catch(_){ caiu=true; }   // deu erro = o processo antigo caiu, agora espera voltar
    if(tentativas<80) setTimeout(tentar,1000);   // até ~80s (relançador espera a porta)
    else{
      ov.querySelector("div:nth-child(2)").textContent="Quase lá…";
      ov.querySelector("div:nth-child(3)").innerHTML=
        "Se esta tela não sair sozinha, clique em <b>Recarregar agora</b> ou aperte F5.";
    }
  };
  setTimeout(tentar, 3000);  // dá tempo do processo começar a cair
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
  if(b==="bambu"){ wiz.step="bambu_modo"; renderAdd(); }
  else if(b==="anycubic"){ wiz.step="anycubic"; renderAdd(); }
  else if(b==="flashforge"){ wiz.step="flashforge"; renderAdd(); }
}
function wizTab(t){ wiz.tab=t; wiz.needCode=false; renderAdd(); }

/* ── Flashforge AD5X (rede local) ─────────────────────── */
async function scanFlashforge(){
  const box=document.getElementById("ffAchadas");
  if(box) box.innerHTML=`<div class="bl-res bl-testando">🔍 Procurando… (leva uns 6 segundos)</div>`;
  try{
    const d=await (await fetch("/api/flashforge/buscar")).json();
    if(!box) return;
    const lista=d.impressoras||[];
    if(!lista.length){
      box.innerHTML=`<div class="bl-res bl-erro">Nenhuma Flashforge encontrada na rede.<br>
        <small>Confira se está ligada, na mesma rede e com o Modo LAN ativo.
        Se não aparecer, adicione pelo IP abaixo.</small></div>`;
      return;
    }
    box.innerHTML=`<div class="bl-lista">`+lista.map(p=>`
      <div class="bl-item" onclick="usarFlashforgeAchada('${escq(p.ip)}','${escq(p.serial||"")}','${escq(p.nome||"")}')">
        <div class="bl-i-nome">${p.nome||"Flashforge"}</div>
        <div class="bl-i-det">${p.ip}${p.serial?" · série "+p.serial:""}${p.modelo?" · "+p.modelo:""}</div>
      </div>`).join("")+`</div>
      <div class="wiz-hint" style="margin-top:.3rem">Clique na impressora para preencher os campos. Depois digite o código dela.</div>`;
  }catch(_){
    if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ Erro ao buscar.</div>`;
  }
}
function usarFlashforgeAchada(ip, serial, nome){
  wiz.ff_ip=ip; wiz.ff_serial=serial||"";
  const c=document.getElementById("ff_code"); wiz.ff_code=c?c.value:"";
  wiz.ff_nome=nome||"";
  renderAdd();
  setTimeout(()=>{ const f=document.getElementById("ff_code"); if(f) f.focus(); },80);
}
function _ffCampos(){
  const g=id=>(document.getElementById(id)?.value||"").trim();
  wiz.ff_ip=g("ff_ip"); wiz.ff_serial=g("ff_serial");
  wiz.ff_code=g("ff_code"); wiz.ff_nome=g("ff_nome");
  return {ip:wiz.ff_ip, serial:wiz.ff_serial, check_code:wiz.ff_code, nome:wiz.ff_nome};
}
async function testarFlashforge(){
  const c=_ffCampos();
  const err=document.getElementById("wizErr");
  const box=document.getElementById("ffResultado");
  if(err) err.textContent="";
  if(!c.ip || !c.check_code){
    if(err) err.textContent="Preencha ao menos o IP e o código de verificação.";
    return;
  }
  if(box) box.innerHTML=`<div class="bl-res bl-testando">🔎 Testando conexão com a impressora…</div>`;
  try{
    const r=await fetch("/api/flashforge/testar",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(c)});
    const d=await r.json();
    if(!d.ok){
      if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ ${d.error||"Não consegui conectar."}</div>`;
      return;
    }
    if(box) box.innerHTML=`<div class="bl-res bl-ok">✓ Conectou! ${d.info||""}</div>`;
    // adiciona de fato
    const add=await fetch("/api/flashforge/add",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(c)});
    const ad=await add.json();
    if(ad.ok){ closeAdd(); if(typeof carregarImpressoras==="function") carregarImpressoras(); }
    else if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ ${ad.error||"Erro ao adicionar."}</div>`;
  }catch(e){
    if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ Erro ao testar.</div>`;
  }
}

/* ── Bambu pelo IP (rede local) ───────────────────────── */
function _blCampos(){
  const g=id=>(document.getElementById(id)?.value||"").trim();
  wiz.lan_ip=g("bl_ip"); wiz.lan_code=g("bl_code");
  wiz.lan_serial=g("bl_serial").toUpperCase(); wiz.lan_nome=g("bl_nome");
  return {ip:wiz.lan_ip, access_code:wiz.lan_code, serial:wiz.lan_serial, nome:wiz.lan_nome};
}
function _blErro(msg){
  const e=document.getElementById("wizErr");
  if(e) e.textContent=msg||"";
  const r=document.getElementById("blResultado");
  if(r && msg) r.innerHTML="";
}
async function buscarBambuLan(){
  const box=document.getElementById("blAchadas");
  _blErro("");
  if(box) box.innerHTML=`<div class="bl-res bl-testando">🔍 Procurando… (leva uns 6 segundos)</div>`;
  try{
    const d=await (await fetch("/api/bambu/buscar_lan")).json();
    if(!box) return;
    if(!d.ok){
      box.innerHTML=`<div class="bl-res bl-erro">✕ ${d.error||"Não consegui buscar."}</div>`;
      return;
    }
    const lista=d.impressoras||[];
    if(!lista.length){
      box.innerHTML=`<div class="bl-res bl-erro">Nenhuma impressora encontrada.<br>
        <small>Confira se ela está ligada, na mesma rede, e com o Modo LAN ativado.
        Se não aparecer, preencha os campos à mão.</small></div>`;
      return;
    }
    box.innerHTML=`<div class="bl-lista">`+lista.map(p=>`
      <div class="bl-item" onclick="usarBambuAchada('${escq(p.ip)}','${escq(p.serial)}','${escq(p.nome||"")}')">
        <div class="bl-i-nome">${p.nome||"Bambu Lab"}</div>
        <div class="bl-i-det">${p.ip} · série ${p.serial}${p.modelo?" · "+p.modelo:""}</div>
      </div>`).join("")+`</div>
      <div class="wiz-hint" style="margin-top:.3rem">Clique na impressora para preencher os campos.</div>`;
  }catch(_){
    if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ Erro ao buscar.</div>`;
  }
}
function usarBambuAchada(ip, serial, nome){
  wiz.lan_ip=ip; wiz.lan_serial=serial;
  const c=document.getElementById("bl_code");
  wiz.lan_code=c?c.value:"";
  const n=document.getElementById("bl_nome");
  wiz.lan_nome=(n&&n.value)?n.value:(nome||"");
  renderAdd();
  setTimeout(()=>{ const f=document.getElementById("bl_code"); if(f) f.focus(); },80);
}
async function testarBambuLan(){
  const c=_blCampos();
  _blErro("");
  if(!c.ip||!c.access_code){
    _blErro("Preencha o IP e o Código de Acesso.");
    return;
  }
  const box=document.getElementById("blResultado");
  if(!c.serial){
    if(box) box.innerHTML=`<div class="bl-res bl-testando">🔎 Descobrindo o número de série…</div>`;
    try{
      const rr=await fetch("/api/bambu/descobrir_serial",{method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({ip:c.ip, access_code:c.access_code})});
      const dd=await rr.json();
      if(!dd.ok){
        if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ ${dd.error||"Não consegui descobrir o número de série."}</div>`;
        return;
      }
      c.serial=dd.serial; wiz.lan_serial=dd.serial;
      const campo=document.getElementById("bl_serial");
      if(campo) campo.value=dd.serial;
    }catch(_){
      if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ Erro ao descobrir o número de série.</div>`;
      return;
    }
  }
  if(box) box.innerHTML=`<div class="bl-res bl-testando">⏳ Testando… (pode levar alguns segundos)</div>`;
  try{
    const r=await fetch("/api/bambu/testar_lan",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({ip:c.ip,access_code:c.access_code,serial:c.serial})});
    const d=await r.json();
    if(!box) return;
    if(d.ok){
      const mod=(d.dados&&d.dados.modelo)?` · modelo ${d.dados.modelo}`:"";
      box.innerHTML=`<div class="bl-res bl-ok">✓ Conectado com sucesso!${mod}<br>
        <small>Pode clicar em "Adicionar".</small></div>`;
    }else{
      box.innerHTML=`<div class="bl-res bl-erro">✕ ${d.mensagem||d.error||"Não consegui conectar."}</div>`;
    }
  }catch(_){
    if(box) box.innerHTML=`<div class="bl-res bl-erro">✕ Erro ao testar.</div>`;
  }
}
async function salvarBambuLan(){
  const c=_blCampos();
  _blErro("");
  if(!c.ip||!c.access_code){
    _blErro("Preencha o IP e o Código de Acesso.");
    return;
  }
  // número de série é opcional: se faltar, o sistema descobre pelo IP
  if(!c.serial){
    const box=document.getElementById("blResultado");
    if(box) box.innerHTML=`<div class="bl-res bl-testando">🔎 Descobrindo o número de série…</div>`;
    try{
      const r=await fetch("/api/bambu/descobrir_serial",{method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({ip:c.ip, access_code:c.access_code})});
      const d=await r.json();
      if(!d.ok){
        if(box) box.innerHTML="";
        _blErro(d.error||"Não consegui descobrir o número de série.");
        return;
      }
      c.serial=d.serial; wiz.lan_serial=d.serial;
      const campo=document.getElementById("bl_serial");
      if(campo) campo.value=d.serial;
      if(box) box.innerHTML=`<div class="bl-res bl-ok">✓ Número de série encontrado: ${d.serial}</div>`;
    }catch(_){
      _blErro("Erro ao descobrir o número de série.");
      return;
    }
  }
  const nome=c.nome||("Bambu "+c.serial.slice(-4));
  try{
    const r=await fetch("/api/printer/add",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({name:nome, brand:"bambu", mode:"lan",
        ip:c.ip, access_code:c.access_code, serial:c.serial})});
    const d=await r.json();
    if(!d.ok){ _blErro(d.error||"Não consegui adicionar."); return; }
    closeAdd();
    nav("printers");
  }catch(_){ _blErro("Erro ao adicionar."); }
}
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
          <div class="brand-card" onclick="wizBrand('bambu')"><div class="bn">Bambu Lab</div><div class="bs">nuvem ou rede local</div></div>
          <div class="brand-card" onclick="wizBrand('anycubic')"><div class="bn">Anycubic / Kobra</div><div class="bs">via rede local</div></div>
          <div class="brand-card" onclick="wizBrand('flashforge')"><div class="bn">Flashforge</div><div class="bs">AD5X · via rede local</div></div>
        </div>
        <div class="wiz-hint">Bambu: pela conta (nuvem) ou direto pelo IP.<br>Anycubic e Flashforge: pela rede local (Modo LAN ativado).</div>
      </div>`;
  } else if(wiz.step==="bambu_modo"){
    m.innerHTML=`
      <div class="wiz-head"><b>Bambu Lab · como conectar</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        <label>Escolha a forma de conexão</label>
        <div class="brands">
          <div class="brand-card" onclick="wiz.step='bambu_lan';renderAdd()">
            <div class="bn">🏠 Pelo IP</div><div class="bs">rede local</div></div>
          <div class="brand-card" onclick="wiz.step='bambu';wiz.tab='login';wiz.needCode=false;renderAdd()">
            <div class="bn">☁ Pela conta</div><div class="bs">nuvem Bambu</div></div>
        </div>
        <div class="wiz-hint" style="margin-top:1rem">
          <b>Pelo IP (recomendado):</b> conexão direta com a impressora, mais rápida
          e continua funcionando sem internet. Precisa que o computador esteja na
          mesma rede da impressora.<br><br>
          <b>Pela conta:</b> detecta todas as impressoras da sua conta Bambu de uma vez.
          Funciona de qualquer lugar, mas depende da internet e dos servidores da Bambu.
        </div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='brand';renderAdd()">Voltar</button>
      </div>`;
  } else if(wiz.step==="bambu_lan"){
    m.innerHTML=`
      <div class="wiz-head"><b>Bambu Lab · pelo IP</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        <div class="auto-detect-box">
          <div class="auto-detect-label">Onde achar os dados na impressora</div>
          <div class="wiz-hint" style="margin:0">
            Na tela da impressora: <b>Configurações (⚙) → Rede</b>.<br>
            Ative o <b>Modo LAN</b> — ali aparecem o <b>IP</b> e o <b>Código de Acesso</b>.<br>
            O <b>Número de Série</b> fica em <b>Configurações → Sobre</b>.
          </div>
        </div>
        <button class="auto-detect-btn" onclick="buscarBambuLan()" style="width:100%">
          🔍 Buscar impressoras na rede
        </button>
        <div class="wiz-hint" style="margin-top:.4rem">
          Encontra as Bambu ligadas na sua rede e preenche o IP e o número de série
          sozinho. O Código de Acesso continua sendo digitado por você.
        </div>
        <div id="blAchadas"></div>
        <div class="wiz-divider"><span>ou preencha à mão</span></div>
        <label>IP da impressora</label>
        <input id="bl_ip" placeholder="192.168.1.20" value="${wiz.lan_ip||''}">
        <label>Código de Acesso</label>
        <input id="bl_code" placeholder="8 caracteres" value="${wiz.lan_code||''}">
        <label>Número de Série <span style="color:var(--faint);font-weight:400">(opcional — descubro sozinho)</span></label>
        <input id="bl_serial" placeholder="deixe vazio que eu procuro" value="${wiz.lan_serial||''}">
        <label>Nome (como quer chamar essa impressora)</label>
        <input id="bl_nome" placeholder="Ex.: A1 da bancada" value="${wiz.lan_nome||''}">
        <div class="wiz-err" id="wizErr"></div>
        <div id="blResultado"></div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='bambu_modo';renderAdd()">Voltar</button>
        <button class="wiz-btn" onclick="testarBambuLan()">Testar conexão</button>
        <button class="wiz-btn primary" onclick="salvarBambuLan()">Adicionar</button>
      </div>`;
  } else if(wiz.step==="bambu"){
    const avancado = wiz.tab==="token";
    const metodo = wiz.metodo||"senha";   // "senha" ou "codigo"
    let botao = "Entrar e detectar";
    if(wiz.needCode) botao = "Confirmar código";
    if(wiz.needTfa)  botao = "Confirmar";
    if(metodo==="codigo" && !wiz.needCode && !wiz.needTfa) botao = "Enviar código para o e-mail";
    m.innerHTML=`
      <div class="wiz-head"><b>Bambu Lab · entrar na conta</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        ${wiz.aviso?`<div class="bl-res bl-testando" style="margin-bottom:.9rem">${wiz.aviso}</div>`:``}
        ${avancado?`
          <label>Token</label>
          <input id="w_token" placeholder="AQB...">
          <label>UID (opcional — detectado automaticamente)</label>
          <input id="w_uid" placeholder="u_1234567">
          <label>Região</label>
          <select id="w_region"><option value="us">Global (fora da China)</option><option value="cn">China</option></select>
          <div class="wiz-hint">Use esta opção só se o login normal não funcionar.
            <a href="#" onclick="wizTab('login');return false" style="color:var(--live)">Voltar ao login por e-mail</a>
          </div>
        `:`
          ${wiz.needCode||wiz.needTfa?``:`
          <div class="wiz-tabs">
            <div class="wiz-tab ${metodo==='senha'?'on':''}" onclick="wizMetodo('senha')">Com senha</div>
            <div class="wiz-tab ${metodo==='codigo'?'on':''}" onclick="wizMetodo('codigo')">Código por e-mail</div>
          </div>
          ${metodo==='codigo'?`<div class="wiz-hint" style="margin-top:.5rem">
            👍 Use esta opção se você criou a conta <b>entrando com o Google</b>
            (ou Apple/Facebook) — nesses casos a conta não tem senha na Bambu.
            Enviaremos um código para o seu e-mail.</div>`:``}
          `}
          <label>E-mail da conta Bambu</label>
          <input id="w_email" type="email" placeholder="seu@email.com" value="${wiz.email||''}">
          ${(metodo==='senha' && !wiz.needCode && !wiz.needTfa)?`
            <label>Senha</label>
            <input id="w_pass" type="password" placeholder="sua senha">`:``}
          ${wiz.needCode?`
            <label>Código recebido por e-mail</label>
            <input id="w_code" placeholder="6 dígitos" autocomplete="one-time-code">
            <div class="wiz-hint">Não chegou? Confira a caixa de spam ou
              <a href="#" onclick="reenviarCodigoBambu();return false" style="color:var(--live)">peça outro código</a>.
            </div>`:``}
          ${wiz.needTfa?`
            <label>Código do aplicativo autenticador</label>
            <input id="w_tfa" placeholder="6 dígitos" autocomplete="one-time-code">`:``}
          <label>Região</label>
          <select id="w_region"><option value="us">Global (fora da China)</option><option value="cn">China</option></select>
          ${(metodo==='senha' && !wiz.needCode && !wiz.needTfa)?`
            <div class="wiz-hint">Se a Bambu pedir um código de verificação, ele será enviado ao seu e-mail.</div>`:``}
        `}
        <div class="wiz-err" id="wizErr"></div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='bambu_modo';wiz.needCode=false;wiz.needTfa=false;wiz.aviso='';renderAdd()">Voltar</button>
        ${avancado?``:`<button class="wiz-btn" onclick="wizTab('token')" title="Para casos em que o login não funciona">Token manual</button>`}
        <button class="wiz-btn primary" onclick="detectBambu()">${botao}</button>
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
  } else if(wiz.step==="flashforge"){
    m.innerHTML=`
      <div class="wiz-head"><b>Flashforge AD5X · modo LAN</b><div class="mclose" onclick="closeAdd()">✕</div></div>
      <div class="wiz-body">
        <div class="auto-detect-box">
          <div class="auto-detect-label">Antes de começar</div>
          <div class="wiz-hint" style="margin:0">
            <b>1.</b> Na tela da impressora: <b>Configurações → Rede → Modo LAN</b> → ative.<br>
            <b>2.</b> Anote o <b>código (Printer ID)</b> que aparece nessa tela de rede.
          </div>
        </div>
        <button class="auto-detect-btn" onclick="scanFlashforge()" style="width:100%">
          🔍 Buscar impressoras Flashforge na rede
        </button>
        <div id="ffAchadas"></div>
        <div class="wiz-divider"><span>ou adicione pelo IP</span></div>
        <label>IP da impressora</label>
        <input id="ff_ip" placeholder="192.168.1.20" value="${wiz.ff_ip||''}">
        <label>Número de série <small style="color:var(--faint)">(opcional)</small></label>
        <input id="ff_serial" placeholder="ex: SNADVXXXXXXX" value="${wiz.ff_serial||''}">
        <label>Código / Printer ID</label>
        <input id="ff_code" placeholder="o código da tela de rede" value="${wiz.ff_code||''}">
        <label>Apelido <small style="color:var(--faint)">(opcional)</small></label>
        <input id="ff_nome" placeholder="ex: Flashforge da bancada" value="${wiz.ff_nome||''}">
        <div class="wiz-hint">A AD5X não tem câmera, então o vídeo não fica disponível — mas status, temperatura, progresso e as cores dos filamentos funcionam.</div>
        <div class="wiz-err" id="wizErr"></div>
        <div id="ffResultado"></div>
      </div>
      <div class="wiz-foot">
        <button class="wiz-btn" onclick="wiz.step='brand';renderAdd()">Voltar</button>
        <button class="wiz-btn primary" onclick="testarFlashforge()">Testar e conectar</button>
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

function wizMetodo(mode){
  wiz.metodo=mode; wiz.needCode=false; wiz.needTfa=false; wiz.aviso="";
  renderAdd();
}
async function detectBambu(){
  const region=val("w_region")||"us";
  const body={brand:"bambu",region};
  const metodo=wiz.metodo||"senha";
  if(wiz.tab==="token"){
    body.uid=val("w_uid"); body.token=val("w_token");
    if(!body.token){ wizErr("Cole o token."); return; }
  }else{
    body.email=val("w_email"); wiz.email=body.email;
    if(!body.email){ wizErr("Digite o e-mail da sua conta Bambu."); return; }
    if(wiz.needCode){
      body.code=val("w_code");
      if(!body.code){ wizErr("Digite o código que chegou no seu e-mail."); return; }
    }else if(wiz.needTfa){
      body.tfa_key=wiz.tfaKey; body.tfa_code=val("w_tfa");
      if(!body.tfa_code){ wizErr("Digite o código do aplicativo autenticador."); return; }
    }else if(metodo==="codigo"){
      // conta Google/sem senha: já pede o envio do código direto
      body.solicitar_codigo=true;
    }else{
      body.password=val("w_pass");
      if(!body.password){ wizErr("Digite a senha."); return; }
    }
  }
  wiz.step="detecting"; renderAdd();
  try{
    const r=await fetch("/api/detect",{method:"POST",
      headers:{"Content-Type":"application/json"},body:JSON.stringify(body)});
    const d=await r.json();
    if(d.need_code){
      wiz.step="bambu"; wiz.needCode=true; wiz.needTfa=false;
      wiz.aviso="📧 "+(d.mensagem||"Enviamos um código para o seu e-mail.");
      renderAdd(); return;
    }
    if(d.need_tfa){
      wiz.step="bambu"; wiz.needTfa=true; wiz.needCode=false;
      wiz.tfaKey=d.tfa_key;
      wiz.aviso="🔐 "+(d.mensagem||"Digite o código do aplicativo autenticador.");
      renderAdd(); return;
    }
    if(!d.ok){ wiz.step="bambu"; renderAdd(); wizErr(d.error||"Falha na detecção."); return; }
    wiz.aviso=""; wiz.needCode=false; wiz.needTfa=false;
    wiz.creds={region:d.region,uid:d.uid,token:d.token};
    wiz.devices=d.printers||[]; wiz.sel=new Set(); wiz.step="devices"; renderAdd();
  }catch(_){ wiz.step="bambu"; renderAdd(); wizErr("Erro de conexão."); }
}
async function reenviarCodigoBambu(){
  const email=val("w_email")||wiz.email||"";
  const region=val("w_region")||"us";
  if(!email){ wizErr("Digite o e-mail primeiro."); return; }
  wiz.aviso="⏳ Pedindo um novo código…"; renderAdd();
  try{
    const r=await fetch("/api/bambu/reenviar_codigo",{method:"POST",
      headers:{"Content-Type":"application/json"},
      body:JSON.stringify({email,region})});
    const d=await r.json();
    wiz.aviso = d.ok ? "📧 Novo código enviado para "+email
                     : "⚠️ "+(d.error||"Não consegui reenviar.");
  }catch(_){ wiz.aviso="⚠️ Erro ao reenviar."; }
  renderAdd();
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
