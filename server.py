# servidor MiniCoin - blockchain simplificada
# implementa uma moeda virtual didatica baseada em lista encadeada de blocos
# suporta criacao de contas, depositos, saques, transferencias e verificacao de integridade
# comunicacao via TCP (linha a linha, respostas em JSON)
# autores: Joao Meyer e Vitor Faria

import socket
import threading
import json
from datetime import datetime, timezone
import hashlib
import sys
import argparse

# SECAO DOS LOGS ---------------------
def log(msg: str) -> None:
    print(msg, flush=True)

def log_banner_inicio(host, port):
    print("=====================================")
    print("inicio da execucao: servidor MiniCoin")
    print("=====================================")
    print(f"MiniCoin TCP server em {host}:{port}\n")

def log_lista_vazia_ou_contas():
    # mostra as contas atualmente ativas
    with ACCOUNTS_LOCK:
        if not ACCOUNTS:
            print("nao ha contas ativas.")
        else:
            donos = " ".join(sorted(ACCOUNTS.keys()))
            print(f"contas ativas: [ {donos} ]")

def log_ver_cadeia(owner: str, chain_list):
    # exibe os indices da cadeia de um usuario
    idxs = " ".join(str(b['index']) for b in chain_list)
    print(f"veja a cadeia de '{owner}': [ {idxs} ]")

def log_erro(msg: str):
    print(f"erro: {msg}", file=sys.stdout, flush=True)

# SECAO DA BLOCKCHAIN -------------
def _utcnow_iso() -> str:
    # retorna timestamp utc no formato iso
    return datetime.now(timezone.utc).isoformat()

def _sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _compute_hash_record(rec: dict) -> str:
    # calcula o hash canonico de um bloco
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
    # cria um registro (bloco) com dados basicos
    rec = {
        "operation": operation,
        "amount": amount,
        "timestamp": _utcnow_iso(),
        "owner": owner,
        "prev_hash": prev_hash,
        "curr_hash": None,
        "next": None
    }
    rec["curr_hash"] = _compute_hash_record(rec)
    return rec

def new_chain(owner: str, initial_deposit: int) -> dict:
    # cria nova cadeia com bloco genesis
    if initial_deposit <= 0:
        raise ValueError("deposito inicial deve ser > 0")
    lock = threading.RLock()
    genesis = _make_record("GENESIS", initial_deposit, owner=owner, prev_hash="0"*64)
    return {"owner": owner, "head": genesis, "tail": genesis, "balance": initial_deposit, "lock": lock}

def _append(chain: dict, rec: dict) -> None:
    # adiciona novo bloco ao final da cadeia
    tail = chain["tail"]
    rec["prev_hash"] = tail["curr_hash"]
    rec["curr_hash"] = _compute_hash_record(rec)
    tail["next"] = rec
    chain["tail"] = rec

def get_balance(chain: dict) -> int:
    with chain["lock"]:
        return chain["balance"]

def _ensure_integrity_or_raise(chain: dict) -> None:
    # verifica integridade antes de alterar
    ok, idx, reason = verify_integrity(chain)
    if not ok:
        raise RuntimeError(f"cadeia corrompida no indice {idx}: {reason}")

def deposit(chain: dict, amount: int) -> dict:
    # adiciona valor a uma conta existente
    if amount <= 0:
        raise ValueError("deposito deve ser > 0")
    with chain["lock"]:
        _ensure_integrity_or_raise(chain)
        rec = _make_record("DEPOSIT", amount)
        _append(chain, rec)
        chain["balance"] += amount
        return rec

def withdraw(chain: dict, amount: int):
    # remove valor da conta, se houver saldo
    if amount <= 0:
        return False, None, "valor de retirada deve ser > 0"
    with chain["lock"]:
        _ensure_integrity_or_raise(chain)
        if amount > chain["balance"]:
            return False, None, "saldo insuficiente"
        rec = _make_record("WITHDRAW", amount)
        _append(chain, rec)
        chain["balance"] -= amount
        return True, rec, "retirada efetuada"

def iter_records(chain: dict):
    # percorre a lista encadeada de blocos
    cur = chain["head"]
    while cur:
        yield cur
        cur = cur["next"]

def verify_integrity(chain: dict):
    # percorre toda a cadeia e verifica consistencia de hashes
    with chain["lock"]:
        idx = 0
        prev = None
        for rec in iter_records(chain):
            expected_prev = "0"*64 if idx == 0 else (prev["curr_hash"] if prev else None)
            if rec["prev_hash"] != expected_prev:
                return False, idx, "prev_hash nao confere"
            expected_curr = _compute_hash_record(rec)
            if rec["curr_hash"] != expected_curr:
                return False, idx, "curr_hash invalido"
            prev = rec
            idx += 1
        return True, None, None

def to_list(chain: dict):
    # converte a cadeia em lista de blocos simples (para enviar em json)
    with chain["lock"]:
        out, i = [], 0
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
    # transfere fundos entre contas com travas em ordem deterministica
    if amt <= 0:
        return False, "amount deve ser > 0"
    first, second = (src, dst) if src["owner"] <= dst["owner"] else (dst, src)
    with first["lock"]:
        with second["lock"]:
            ok_src, idx_src, reason_src = verify_integrity(src)
            if not ok_src:
                return False, f"cadeia de '{src['owner']}' corrompida no indice {idx_src}: {reason_src}"
            ok_dst, idx_dst, reason_dst = verify_integrity(dst)
            if not ok_dst:
                return False, f"cadeia de '{dst['owner']}' corrompida no indice {idx_dst}: {reason_dst}"
            if amt > src["balance"]:
                return False, "saldo insuficiente"
            rec_w = _make_record("WITHDRAW", amt)
            _append(src, rec_w)
            src["balance"] -= amt
            rec_d = _make_record("DEPOSIT", amt)
            _append(dst, rec_d)
            dst["balance"] += amt
            return True, "transferencia concluida"

# SECAO DO PROTOCOLO TCP -------------------
ACCOUNTS = {}          
ACCOUNTS_LOCK = threading.RLock()

def _resp_bytes(ok=True, **data):
    # monta resposta json terminada em '\n'
    obj = {"ok": ok}
    obj.update(data)
    return (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")

def recvline(sock):
    # le uma linha (terminada em '\n') do socket
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
    # thread que atende um cliente conectado
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

            # comandos basicos
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

            # criacao de conta
            if cmd == "INIT":
                if len(args) != 2:
                    conn.sendall(_resp_bytes(False, error="uso: INIT <owner> <initial>"))
                    log_erro("uso incorreto de INIT")
                    continue
                owner, initial_s = args
                try:
                    initial = int(initial_s)
                except ValueError:
                    conn.sendall(_resp_bytes(False, error="initial deve ser inteiro"))
                    continue
                with ACCOUNTS_LOCK:
                    if owner in ACCOUNTS:
                        conn.sendall(_resp_bytes(False, error=f"conta '{owner}' ja existe"))
                        continue
                    ACCOUNTS[owner] = new_chain(owner, initial)
                    log(f"inseri conta '{owner}' com deposito inicial = {initial}")
                    log_lista_vazia_ou_contas()
                    log_ver_cadeia(owner, to_list(ACCOUNTS[owner]))
                conn.sendall(_resp_bytes(True, owner=owner, balance=initial))
                continue

            # listagem de contas
            if cmd == "ACCOUNTS":
                with ACCOUNTS_LOCK:
                    owners = sorted(ACCOUNTS.keys())
                conn.sendall(_resp_bytes(True, owners=owners))
                log_lista_vazia_ou_contas()
                continue

            # transferencia entre contas
            if cmd == "TRANSFER":
                if len(args) != 3:
                    conn.sendall(_resp_bytes(False, error="uso: TRANSFER <from_owner> <to_owner> <amount>"))
                    continue
                from_owner, to_owner, amt_s = args
                try:
                    amt = int(amt_s)
                except ValueError:
                    conn.sendall(_resp_bytes(False, error="amount deve ser inteiro"))
                    continue
                with ACCOUNTS_LOCK:
                    src = ACCOUNTS.get(from_owner)
                    dst = ACCOUNTS.get(to_owner)
                if not src or not dst:
                    conn.sendall(_resp_bytes(False, error="conta inexistente"))
                    continue
                ok, msg = transfer_atomic(src, dst, amt)
                conn.sendall(_resp_bytes(ok, message=msg, balance_src=get_balance(src), balance_dst=get_balance(dst)))
                continue

            # operacoes simples de conta
            if cmd in ("DEPOSIT", "WITHDRAW", "BALANCE", "CHAIN", "VERIFY"):
                if len(args) < 1:
                    conn.sendall(_resp_bytes(False, error=f"uso: {cmd} <owner> ..."))
                    continue
                owner = args[0]
                with ACCOUNTS_LOCK:
                    chain = ACCOUNTS.get(owner)
                if not chain:
                    conn.sendall(_resp_bytes(False, error=f"conta '{owner}' nao existe"))
                    continue

                if cmd == "DEPOSIT":
                    if len(args) != 2:
                        conn.sendall(_resp_bytes(False, error="uso: DEPOSIT <owner> <amount>"))
                        continue
                    try:
                        amt = int(args[1])
                        deposit(chain, amt)
                        conn.sendall(_resp_bytes(True, owner=owner, balance=get_balance(chain)))
                    except ValueError as e:
                        # erro de valor
                        conn.sendall(_resp_bytes(False, error=str(e)))
                        log_erro(f"erro ao depositar em '{owner}': {e}")
                    except Exception as e:
                        # tratando excessoes desconhecidas
                        conn.sendall(_resp_bytes(False, error=f"falha no deposito: {e}"))
                        log_erro(f"excecao ao depositar em '{owner}': {e}")
                    continue

                if cmd == "WITHDRAW":
                    if len(args) != 2:
                        conn.sendall(_resp_bytes(False, error="uso: WITHDRAW <owner> <amount>"))
                        continue
                    amt = int(args[1])
                    ok, _, msg = withdraw(chain, amt)
                    conn.sendall(_resp_bytes(ok, owner=owner, balance=get_balance(chain), message=msg))
                    continue

                if cmd == "BALANCE":
                    conn.sendall(_resp_bytes(True, owner=owner, balance=get_balance(chain)))
                    continue

                if cmd == "CHAIN":
                    conn.sendall(_resp_bytes(True, owner=owner, chain=to_list(chain)))
                    continue

                if cmd == "VERIFY":
                    ok, idx, reason = verify_integrity(chain)
                    conn.sendall(_resp_bytes(ok, owner=owner, index=idx, reason=reason))
                    continue

            # comando desconhecido
            conn.sendall(_resp_bytes(False, error=f"comando desconhecido: {cmd}"))
    except Exception as e:
        log_erro(f"excecao na conexao {addr}: {e}")
    finally:
        conn.close()

def run(host="127.0.0.1", port=9090, backlog=100):
    # loop principal do servidor tcp
    log_banner_inicio(host, port)
    srv_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv_sock.bind((host, port))
    srv_sock.listen(backlog)
    try:
        while True:
            conn, addr = srv_sock.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        srv_sock.close()

# MAIN -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="servidor tcp simples")
    parser.add_argument("--host", default="127.0.0.1", help="endereco para bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9090, help="porta para bind (default: 9090)")
    args = parser.parse_args()
    run(host=args.host, port=args.port)
