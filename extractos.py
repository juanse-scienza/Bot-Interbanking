import os
import sys
from datetime import date
import requests
import pandas as pd
from pandas.tseries.offsets import BDay
from dotenv import load_dotenv

# ─── 1) Entorno ───────────────────────────────────────────────────────────────────────────
load_dotenv()
CLIENT_ID     = os.getenv("IB_CLIENT_ID")
CLIENT_SECRET = os.getenv("IB_CLIENT_SECRET")
SERVICE_URL   = os.getenv("IB_SERVICE_URL", "https://localhost/callback")
CUSTOMER_ID   = os.getenv("CUSTOMER_ID")

if not CLIENT_ID or not CLIENT_SECRET or not CUSTOMER_ID:
    print("⚠️  Define en .env: IB_CLIENT_ID, IB_CLIENT_SECRET y CUSTOMER_ID")
    sys.exit(1)

# ─── 2) Endpoints ───────────────────────────────────────────────────────────────────────
TOKEN_URL = "https://auth.interbanking.com.ar/cas/oidc/accessToken"
STMT_URL  = "https://api-gw.interbanking.com.ar/api/prod/v1/accounts/{account_number}/statements"


# ─── 3) Obtener Token ───────────────────────────────────────────────────────────────────────
def obtener_token() -> str:
    """
    Obtiene el access_token usando OAuth2 Client Credentials:
      • client_id/client_secret en params y header service
      • body form-encoded: grant_type=client_credentials
    """
    params = {
        "scope":         "info-financiera",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {"service": SERVICE_URL}
    data = {"grant_type": "client_credentials"}
    resp = requests.post(TOKEN_URL, params=params, headers=headers, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]


# ─── 4) Descarga de extractos ───────────────────────────────────────────────────────────────────────
def descargar_extractos(token: str, account_number: str, bank_number: str, fecha: str) -> list[dict]:
    headers = {
        "Authorization": f"Bearer {token}",
        "client_id":     CLIENT_ID
    }
    params = {
        "account-type": "CC",
        "bank-number":  bank_number.zfill(3),
        "currency":     "ARS",
        "customer-id":  CUSTOMER_ID,
        "date-since":   fecha,
        "date-until":   fecha
    }
    url = STMT_URL.format(account_number=account_number)
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("statements", []) if isinstance(data, dict) else data

# ─── 5) Estandarizar ───────────────────────────────────────────────────────────────────────
def estandarizar(movs: list[dict]) -> pd.DataFrame:
    if not movs:
        return pd.DataFrame()
    df = pd.json_normalize(movs)
    date_cols = [c for c in df.columns if "date" in c.lower()]
    if date_cols:
        df = df.rename(columns={date_cols[0]: "Fecha"})
    mapping = {
        "description":  "Concepto",
        "debitAmount":  "Debito",
        "creditAmount": "Credito",
        "balance":      "Saldo",
        "bankId":       "Banco"
    }
    df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"])
        df = df.sort_values("Fecha")
    key_cols = [c for c in ["Fecha","Concepto","Debito","Credito","Saldo"] if c in df.columns]
    return df.drop_duplicates(subset=key_cols).reset_index(drop=True)

# ─── 6) Calcular ultimo dia habil ───────────────────────────────────────────────────────────────────────
def ultimo_dia_habil(hoy: date) -> date:
    return (pd.Timestamp(hoy) - BDay(1)).date()

# ─── 7) Ejecución de programa principal ─────────────────────────────────────────────────────────────────
def main():
    bancos = (
        pd.read_excel("bancos.xlsx", dtype=str)
          .rename(columns={"N°": "bank_number", "Cuenta": "account_number"})
    )

    fecha = ultimo_dia_habil(date.today()).isoformat()

    carpeta_mov   = f"Movimientos {fecha}"
    carpeta_nomov = f"Sin movimientos {fecha}"
    os.makedirs(carpeta_mov, exist_ok=True)
    os.makedirs(carpeta_nomov, exist_ok=True)

    token = obtener_token()

    sin_mov = []

    for _, b in bancos.iterrows():
        entidad = b.get("Entidad", b.bank_number)
        movs = descargar_extractos(
            token,
            account_number=b.account_number,
            bank_number=   b.bank_number,
            fecha=          fecha
        )
        df = estandarizar(movs)

        if df.empty:
            print(f"{entidad}: 0 movimientos")
            sin_mov.append(entidad)
        else:
            print(f"{entidad}: {len(df)} movimientos — exportando…")
            filename = f"{entidad} Movimientos - Fecha {fecha}.xlsx"
            ruta = os.path.join(carpeta_mov, filename)
            df.to_excel(ruta, index=False, sheet_name="Movimientos")
            print(f"  ✔️  Guardado en «{ruta}»")
        print()

    log_path = os.path.join(carpeta_nomov, "log.txt")
    with open(log_path, "w", encoding="utf-8") as log_file:
        log_file.write("Bancos sin movimientos en " + fecha + "\n")
        log_file.write("===============================\n")
        for ent in sin_mov:
            log_file.write(ent + "\n")
    print(f"Log de bancos sin movimientos guardado en «{log_path}»")

if __name__ == "__main__":
    main()
