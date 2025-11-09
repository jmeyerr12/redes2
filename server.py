#   MiniCoin — Servidor TCP com Blockchain Simplificada 
#   Objetivo: implementar uma moeda virtual didática baseada em lista encadeada de registros (blockchain),
#             com criação de contas (bloco gênese), depósitos, saques com validação de saldo, transferências
#             entre contas e verificação de integridade encadeada via SHA-256. Protocolo linha-a-linha,
#             respostas em JSON por linha.
#   Restrições/escopo: um único servidor mantém todas as cadeias em memória; entradas numéricas inteiras;
#             sem persistência em disco; sem segurança de transporte; atendimento concorrente por threads.
#
#   Autores originais: João Meyer e Vitor Faria
#   Disciplina: Redes II

import socket
import threading
import json
from datetime import datetime, timezone
import hashlib
import sys

# ----------------- LOGS -----------------
def log(msg: str) -> None:
    print(msg, flush=True)

def log_banner_inicio(host, port):
    print("=====================================")
    print("Inicio da execucao: servidor MiniCoin")
    print("=====================================")
    print(f"MiniCoin TCP server em {host}:{port}")
    print()

def log_lista_vazia_ou_contas():
    with ACCOUNTS_LOCK:
        if not ACCOUNTS:
            print("Nao ha contas ativas.")
        else:
            donos = " ".join(sorted(ACCOUNTS.keys()))
            print(f"Contas ativas: [ {donos} ]")

def log_ver_cadeia(owner: str, chain_list):
    idxs = " ".join(str(b['index']) for b in chain_list)
    print(f"Veja a Cadeia de '{owner}': [ {idxs} ]")

def log_erro(msg: str):
    print(f"ERRO: {msg}", file=sys.stdout, flush=True)

# ----------------- CORE (procedural) -----------------
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _compute_hash_record(rec: dict) -> str:
    payload = {
        "owner": rec.get("owner") or "",
        "operation": rec["operation"],
        "amount": rec["amount"],
        "timestamp": rec["timestamp"],
        "prev_hash": rec.get("prev_hash") or ""
    }
    canon = json.dumps(payload, separators=(',', ':'), sort_keys=True)
    return _sha256_str(canon)

def _make_record(operation: str, amount: int, owner=None, prev_hash=None) -> dict:
    rec = {
        "operation": operation,   # "GENESIS" | "DEPOSIT" | "WITHDRAW"
        "amount": amount,
        "timestamp": _utcnow_iso(),
        "owner": owner,           # só no gênese; nos demais fica None
        "prev_hash": prev_hash,   # antes de calcular curr_hash
        "curr_hash": None,
        "next": None
    }
    rec["curr_hash"] = _compute_hash_record(rec)
    return rec

def new_chain(owner: str, initial_deposit: int) -> dict:
    if initial_deposit <= 0:
        raise ValueError("Depósito inicial deve ser > 0")
    lock = threading.RLock()
    genesis = _make_record("GENESIS", initial_deposit, owner=owner, prev_hash="0"*64)
    return {
        "owner": owner,
        "head": genesis,
        "tail": genesis,
        "balance": initial_deposit,
        "lock": lock
    }

def _append(chain: dict, rec: dict) -> None:
    # requer que o lock da cadeia já esteja adquirido
    tail = chain["tail"]
    rec["prev_hash"] = tail["curr_hash"]
    rec["curr_hash"] = _compute_hash_record(rec)
    tail["next"] = rec
    chain["tail"] = rec

def get_balance(chain: dict) -> int:
    with chain["lock"]:
        return chain["balance"]

def _ensure_integrity_or_raise(chain: dict) -> None:
    ok, idx, reason = verify_integrity(chain)
    if not ok:
        raise RuntimeError(f"cadeia corrompida no indice {idx}: {reason}")

def deposit(chain: dict, amount: int) -> dict:
    if amount <= 0:
        raise ValueError("Depósito deve ser > 0")
    with chain["lock"]:
        # valida integridade ANTES de modificar
        _ensure_integrity_or_raise(chain)
        rec = _make_record("DEPOSIT", amount)
        _append(chain, rec)
        chain["balance"] += amount
        return rec

def withdraw(chain: dict, amount: int):
    if amount <= 0:
        return False, None, "Valor de retirada deve ser > 0"
    with chain["lock"]:
        # valida integridade ANTES de modificar
        _ensure_integrity_or_raise(chain)
        if amount > chain["balance"]:
            return False, None, "impossivel retirar, saldo insuficiente!"
        rec = _make_record("WITHDRAW", amount)
        _append(chain, rec)
        chain["balance"] -= amount
        return True, rec, "Retirada efetuada."

def iter_records(chain: dict):
    cur = chain["head"]
    while cur:
        yield cur
        cur = cur["next"]

def verify_integrity(chain: dict):
    # varredura completa da cadeia
    with chain["lock"]:
        idx = 0
        prev = None
        for rec in iter_records(chain):
            expected_prev = "0"*64 if idx == 0 else (prev["curr_hash"] if prev else None)
            if rec["prev_hash"] != expected_prev:
                return False, idx, "prev_hash nao confere!"
            expected_curr = _compute_hash_record(rec)
            if rec["curr_hash"] != expected_curr:
                return False, idx, "curr_hash invalido!"
            prev = rec
            idx += 1
        return True, None, None

def to_list(chain: dict):
    with chain["lock"]:
        out = []
        i = 0
        for r in iter_records(chain):
            out.append({
                "index": i,
                "operation": r["operation"],
                "owner": r.get("owner"),
                "amount": r["amount"],
                "timestamp": r["timestamp"],
                "prev_hash": r["prev_hash"],
                "curr_hash": r["curr_hash"],
            })
            i += 1
        return out

def transfer_atomic(src: dict, dst: dict, amt: int):
    """
    Transfere de src -> dst de forma atômica:
    - Bloqueia as duas cadeias em ordem determinística para evitar deadlock
    - Verifica integridade de ambas
    - Checa saldo
    - Aplica WITHDRAW em src e DEPOSIT em dst
    Retorna: (ok: bool, msg: str)
    """
    if amt <= 0:
        return False, "amount deve ser > 0"

    # Ordem determinística de locks para evitar deadlock
    # usa o nome do dono (estável e legível)
    first, second = (src, dst) if src["owner"] <= dst["owner"] else (dst, src)

    with first["lock"]:
        with second["lock"]:
            # verifica integridade antes de qualquer modificação
            ok_src, idx_src, reason_src = verify_integrity(src)
            if not ok_src:
                return False, f"cadeia de '{src['owner']}' corrompida no indice {idx_src}: {reason_src}"
            ok_dst, idx_dst, reason_dst = verify_integrity(dst)
            if not ok_dst:
                return False, f"cadeia de '{dst['owner']}' corrompida no indice {idx_dst}: {reason_dst}"

            if amt > src["balance"]:
                return False, "impossivel retirar, saldo insuficiente!"

            # aplica saque em src
            rec_w = _make_record("WITHDRAW", amt)
            _append(src, rec_w)
            src["balance"] -= amt

            # aplica deposito em dst
            rec_d = _make_record("DEPOSIT", amt)
            _append(dst, rec_d)
            dst["balance"] += amt

            return True, "transferencia concluida"

# ----------------- REDE / PROTOCOLO -----------------
ACCOUNTS = {}          # owner -> chain dict
ACCOUNTS_LOCK = threading.RLock()

def _resp_bytes(ok=True, **data):
    obj = {"ok": ok}
    obj.update(data)
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")

def recvline(sock):
    data = bytearray()
    while True:
        ch = sock.recv(1)
        if not ch:
            return None
        if ch == b"\n":
            break
        data += ch
    return data.decode("utf-8", errors="replace")

def handle_client(conn, addr):
    try:
        conn.sendall(_resp_bytes(True, message="MiniCoin TCP pronto. Use HELP."))
        while True:
            line = recvline(conn)
            if line is None:
                break
            text = line.strip()
            if not text:
                continue

            parts = text.split()
            cmd = parts[0].upper()
            args = parts[1:]

            if cmd in ("QUIT", "EXIT"):
                conn.sendall(_resp_bytes(True, message="bye"))
                break

            if cmd == "HELP":
                conn.sendall(_resp_bytes(True, commands=[
                    "INIT <owner> <initial>",
                    "DEPOSIT <owner> <amount>",
                    "WITHDRAW <owner> <amount>",
                    "BALANCE <owner>",
                    "CHAIN <owner>",
                    "VERIFY <owner>",
                    "TRANSFER <from_owner> <to_owner> <amount>",
                    "ACCOUNTS",
                    "QUIT"
                ]))
                continue

            if cmd == "INIT":
                if len(args) != 2:
                    conn.sendall(_resp_bytes(False, error="uso: INIT <owner> <initial>"))
                    log_erro("impossivel criar conta, uso correto: INIT <owner> <initial>")
                    continue
                owner, initial_s = args
                try:
                    initial = int(initial_s)
                except ValueError:
                    conn.sendall(_resp_bytes(False, error="initial deve ser inteiro"))
                    log_erro(f"impossivel criar conta '{owner}', valor inicial invalido!")
                    continue

                with ACCOUNTS_LOCK:
                    if owner in ACCOUNTS:
                        conn.sendall(_resp_bytes(False, error=f"conta '{owner}' já existe"))
                        log_erro(f"impossivel criar conta '{owner}', conta ja existe!")
                        continue
                    try:
                        ACCOUNTS[owner] = new_chain(owner, initial)
                        log(f"Inseri conta '{owner}' com deposito inicial = {initial}")
                        log_lista_vazia_ou_contas()
                        log_ver_cadeia(owner, to_list(ACCOUNTS[owner]))
                    except Exception as e:
                        conn.sendall(_resp_bytes(False, error=str(e)))
                        log_erro(f"impossivel criar conta '{owner}', {e}!")
                        continue
                conn.sendall(_resp_bytes(True, owner=owner, balance=initial))
                continue

            if cmd == "ACCOUNTS":
                with ACCOUNTS_LOCK:
                    owners = sorted(ACCOUNTS.keys())
                conn.sendall(_resp_bytes(True, owners=owners))
                if owners:
                    print("Veja as contas ativas: [ " + " ".join(owners) + " ]")
                else:
                    print("Nao ha contas ativas.")
                continue

            if cmd == "TRANSFER":
                if len(args) != 3:
                    conn.sendall(_resp_bytes(False, error="uso: TRANSFER <from_owner> <to_owner> <amount>"))
                    log_erro("impossivel transferir, uso correto: TRANSFER <from_owner> <to_owner> <amount>")
                    continue
                from_owner, to_owner, amt_s = args
                try:
                    amt = int(amt_s)
                except ValueError:
                    conn.sendall(_resp_bytes(False, error="amount deve ser inteiro"))
                    log_erro(f"impossivel transferir {amt_s}, valor invalido!")
                    continue

                with ACCOUNTS_LOCK:
                    src = ACCOUNTS.get(from_owner)
                    dst = ACCOUNTS.get(to_owner)
                if src is None:
                    conn.sendall(_resp_bytes(False, error=f"conta '{from_owner}' não existe"))
                    log_erro(f"impossivel transferir, conta '{from_owner}' inexistente!")
                    continue
                if dst is None:
                    conn.sendall(_resp_bytes(False, error=f"conta '{to_owner}' não existe"))
                    log_erro(f"impossivel transferir, conta '{to_owner}' inexistente!")
                    continue

                ok, msg = transfer_atomic(src, dst, amt)
                if not ok:
                    conn.sendall(_resp_bytes(False, error=f"falha ao transferir: {msg}", balance_src=get_balance(src)))
                    log_erro(f"impossivel transferir {amt} de '{from_owner}' para '{to_owner}', {msg}")
                    continue

                conn.sendall(_resp_bytes(
                    True,
                    message=f"transferência de {amt} MC de {from_owner} para {to_owner} concluída",
                    balance_src=get_balance(src),
                    balance_dst=get_balance(dst)
                ))
                log(f"Transferi {amt} MC: '{from_owner}' -> '{to_owner}'")
                print(f"Saldo '{from_owner}' = {get_balance(src)} | Saldo '{to_owner}' = {get_balance(dst)}")
                continue

            if cmd in ("DEPOSIT", "WITHDRAW", "BALANCE", "CHAIN", "VERIFY"):
                if len(args) < 1:
                    conn.sendall(_resp_bytes(False, error=f"uso: {cmd} <owner> ..."))
                    log_erro(f"uso incorreto de {cmd}")
                    continue
                owner = args[0]
                with ACCOUNTS_LOCK:
                    chain = ACCOUNTS.get(owner)
                if chain is None:
                    conn.sendall(_resp_bytes(False, error=f"conta '{owner}' não existe"))
                    log_erro(f"impossivel operar, conta '{owner}' inexistente!")
                    continue

                if cmd == "DEPOSIT":
                    if len(args) != 2:
                        conn.sendall(_resp_bytes(False, error="uso: DEPOSIT <owner> <amount>"))
                        log_erro("impossivel depositar, uso correto: DEPOSIT <owner> <amount>")
                        continue
                    try:
                        amt = int(args[1])
                        deposit(chain, amt)  # valida integridade internamente
                        conn.sendall(_resp_bytes(True, owner=owner, balance=get_balance(chain)))
                        log(f"Inseri deposito = {amt} na conta '{owner}'")
                        print(f"Saldo '{owner}' = {get_balance(chain)}")
                    except Exception as e:
                        conn.sendall(_resp_bytes(False, error=str(e)))
                        log_erro(f"impossivel depositar em '{owner}', {e}!")
                    continue

                    # WITHDRAW
                if cmd == "WITHDRAW":
                    if len(args) != 2:
                        conn.sendall(_resp_bytes(False, error="uso: WITHDRAW <owner> <amount>"))
                        log_erro("impossivel sacar, uso correto: WITHDRAW <owner> <amount>")
                        continue
                    try:
                        amt = int(args[1])
                    except ValueError:
                        conn.sendall(_resp_bytes(False, error="amount deve ser inteiro"))
                        log_erro("impossivel sacar, valor invalido!")
                        continue
                    ok, _, msg = withdraw(chain, amt)  # valida integridade internamente
                    if ok:
                        conn.sendall(_resp_bytes(True, owner=owner, balance=get_balance(chain)))
                        log(f"Removi {amt} da conta '{owner}'")
                        print(f"Saldo '{owner}' = {get_balance(chain)}")
                    else:
                        conn.sendall(_resp_bytes(False, error=msg, owner=owner, balance=get_balance(chain)))
                        log_erro(f"impossivel remover {amt} da conta '{owner}', {msg}")
                    continue

                if cmd == "BALANCE":
                    # opcional: poderíamos validar aqui também; mantido leve
                    conn.sendall(_resp_bytes(True, owner=owner, balance=get_balance(chain)))
                    print(f"Saldo de '{owner}' = {get_balance(chain)}")
                    continue

                if cmd == "CHAIN":
                    lst = to_list(chain)
                    conn.sendall(_resp_bytes(True, owner=owner, chain=lst))
                    log_ver_cadeia(owner, lst)
                    continue

                if cmd == "VERIFY":
                    ok, idx, reason = verify_integrity(chain)
                    conn.sendall(_resp_bytes(ok, owner=owner, index=idx, reason=reason))
                    if ok:
                        print(f"Cadeia de '{owner}' integra.")
                    else:
                        log_erro(f"Falha de integridade em '{owner}' no indice {idx}: {reason}")
                    continue

            # Comando desconhecido
            conn.sendall(_resp_bytes(False, error=f"comando desconhecido: {cmd}"))
            log_erro(f"comando desconhecido: {cmd}")

    except Exception as e:
        log_erro(f"excecao na conexao {addr}: {e}")
    finally:
        try:
            conn.close()
        except:
            pass

def run(host="127.0.0.1", port=9090, backlog=100):
    log_banner_inicio(host, port)
    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv_sock.bind((host, port))
    srv_sock.listen(backlog)
    try:
        while True:
            conn, addr = srv_sock.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        pass
    finally:
        srv_sock.close()

if __name__ == "__main__":
    run()
